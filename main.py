import asyncio, gzip, hashlib, ipaddress, json, logging, os, re, shutil, signal, socket, subprocess, sys, tempfile, threading, time, uuid, zipfile
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

import yt_dlp
from yt_dlp.utils import DownloadCancelled
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

MAX_MB = int(os.getenv("MAX_FILE_MB", 500))
MAX_MIN = int(os.getenv("MAX_DURATION_MIN", 60))
RATE = int(os.getenv("RATE_LIMIT", 20))          # info/download POSTs per minute per IP
SLOTS = int(os.getenv("MAX_PARALLEL", 2))        # simultaneous downloads (others queue)
GENERIC = os.getenv("ALLOW_GENERIC", "0") == "1"  # arbitrary-URL extractor (SSRF risk), off by default
AUTO_UPDATE = os.getenv("AUTO_UPDATE", "1") == "1"
TTL = 600
HERE = Path(__file__).parent
ROOT = Path(tempfile.gettempdir()) / "grabit"
shutil.rmtree(ROOT, ignore_errors=True)  # orphans from a previous run
ROOT.mkdir(parents=True)
JOBS: dict = {}
KEYS: dict = {}   # (url, quality, playlist, saver) -> job id, so repeat requests reuse the same file
INFO: dict = {}   # url -> (time, result), spares the origin site repeated lookups
HITS: dict = defaultdict(deque)
SLOT = threading.BoundedSemaphore(SLOTS)
log = logging.getLogger("grabit")
BASE = dict(quiet=True, no_warnings=True, noprogress=True, socket_timeout=15, retries=3, fragment_retries=5, cachedir=False,
            allowed_extractors=["default"] if GENERIC else ["default", "-generic"])


class Req(BaseModel):
    url: str = Field(max_length=2048)
    quality: str = "v720"
    playlist: bool = False
    saver: bool = True


class UserError(Exception):
    pass


ERRORS = [
    (("drm",), "This video is DRM-protected and can't be downloaded."),
    (("unsupported url",), "This site isn't supported."),
    (("not a bot",), "The site blocked this server. Try again later."),
    (("confirm your age", "age-restricted", "age restricted"), "This video is age-restricted."),
    (("not available in your country", "geo-restrict", "geo restrict", "blocked it in your", "blocked in your"), "This video is blocked in the server's region."),
    (("sign in", "log in", "login required"), "This video needs a login."),
    (("private",), "This video is private."),
    (("unavailable", "removed", "does not exist", "404"), "This video was removed or is unavailable."),
]


def friendly(e):
    m = str(e).lower()
    for keys, msg in ERRORS:
        if any(k in m for k in keys):
            return msg
    return "Couldn't process that link."


def check_url(u):
    u = u.strip()
    p = urlparse(u)
    if p.scheme not in ("http", "https") or not p.hostname:
        raise UserError("Enter a valid http(s) link.")
    try:
        for i in socket.getaddrinfo(p.hostname, p.port or 80):
            if not ipaddress.ip_address(i[4][0].split("%")[0]).is_global:
                raise UserError("That address isn't allowed.")
    except (socket.gaierror, ValueError):
        raise UserError("Couldn't find that website.")
    return u


def safe(t):
    return re.sub(r'[\\/:*?"<>|\x00-\x1f]', "", t).strip(" .")[:100] or "video"


# ---------- format selection (shared by size estimates and the real download) ----------
def has(f, k):
    return f.get(k) not in (None, "none")


def res(f):  # "720p" means the short side, so vertical videos (Shorts/Reels) are labelled correctly
    h = f.get("height") or 0
    return min(h, f.get("width") or h)


def est(f, dur):
    return (f.get("filesize") or f.get("filesize_approx") or int((f.get("tbr") or 0) * 125 * dur)) if f else 0


def pick_a(auds, saver):
    if not auds:
        return None
    c = [f for f in auds if (f.get("abr") or 0) <= 132] if saver else auds  # >128k audio is wasted data on phones
    c = c or [min(auds, key=lambda f: f.get("abr") or 0)]
    return min(c, key=lambda f: (-(f.get("language_preference") or 0), f.get("ext") != "m4a", -(f.get("abr") or 0)))


def pick(fm, h, saver):
    ok = [f for f in fm if f.get("has_drm") is not True]
    vids = [f for f in ok if has(f, "vcodec") and f.get("height")]
    if not vids:
        return None, None
    pool = [f for f in vids if res(f) <= h] or [min(vids, key=res)]
    top = max(res(f) for f in pool)
    c = [f for f in pool if res(f) == top]
    fps = lambda f: -(f.get("fps") or 0)
    if saver:  # same resolution + fps, smallest encode (AV1/VP9 beat H.264 by 30-50%)
        v = min(c, key=lambda f: (fps(f), f.get("tbr") or 1e9))
    else:      # widest compatibility: H.264 first
        v = min(c, key=lambda f: (fps(f), not str(f.get("vcodec")).startswith("avc1"), -(f.get("tbr") or 0)))
    a = None
    if not has(v, "acodec"):
        a = pick_a([f for f in ok if f.get("vcodec") == "none" and has(f, "acodec")], saver)
    return v, a


def selector(q, saver):
    def sel(ctx):
        fm = ctx["formats"]
        if q[0] == "v":
            v, a = pick(fm, int(q[1:]), saver)
        else:
            v, a = pick_a([f for f in fm if f.get("vcodec") == "none" and has(f, "acodec")], saver), None
        if not v:
            if fm:
                yield fm[-1]
        elif a:
            yield {"format_id": f"{v['format_id']}+{a['format_id']}", "ext": "mp4", "requested_formats": [v, a],
                   "protocol": f"{v.get('protocol')}+{a.get('protocol')}"}
        else:
            yield v
    return sel


def thumb(i):
    ts = sorted((t for t in i.get("thumbnails") or [] if t.get("url") and t.get("width")), key=lambda t: t["width"])
    t = next((t for t in ts if t["width"] >= 320), ts[-1] if ts else None)  # smallest sharp-enough image
    return t["url"] if t else i.get("thumbnail")


def tile(id_, label, size=0, full=0):
    return {"id": id_, "label": label, "size": size, "full": full or size,
            "disabled": bool(size) and min(size, full or size) > MAX_MB * 1_000_000}


def get_info(r):
    url = check_url(r.url)
    hit = INFO.get(url)
    if hit and time.time() - hit[0] < 300:
        return hit[1]
    try:
        with yt_dlp.YoutubeDL({**BASE, "noplaylist": True, "extract_flat": "in_playlist", "playlistend": 10}) as y:
            i = y.extract_info(url, download=False)
    except Exception as e:
        log.warning("info failed: %s", e)
        raise UserError(friendly(e))
    if not i:
        raise UserError("Couldn't process that link.")
    if i.get("_type") == "playlist":
        es = [e for e in (i.get("entries") or []) if e][:10]
        if not es:
            raise UserError("This playlist is empty.")
        out = {"title": i.get("title") or "Playlist", "thumb": next((thumb(e) for e in es if e.get("thumbnails")), None),
               "dur": 0, "by": i.get("uploader"), "playlist": True, "count": len(es),
               "video": [tile(f"v{h}", f"{h}p") for h in (1080, 720, 480, 360)],
               "audio": [tile("m4a", "M4A"), tile("mp3", "MP3")]}
    else:
        if i.get("is_live"):
            raise UserError("Live streams aren't supported.")
        dur = i.get("duration") or 0
        if dur > MAX_MIN * 60:
            raise UserError(f"Videos over {MAX_MIN} minutes aren't supported.")
        fm = [f for f in (i.get("formats") or []) if f.get("has_drm") is not True]
        if i.get("formats") and not fm:
            raise UserError(friendly("drm"))
        top = max((res(f) for f in fm if has(f, "vcodec") and f.get("height")), default=0)
        V, seen = [], set()
        for h in (2160, 1440, 1080, 720, 480, 360, 240):
            if top < h:
                continue
            v, a = pick(fm, h, True)
            if not v or v["format_id"] in seen:
                continue
            seen.add(v["format_id"])
            v2, a2 = pick(fm, h, False)
            V.append(tile(f"v{h}", f"{h}p", est(v, dur) + est(a, dur), est(v2, dur) + est(a2, dur)))
        auds = [f for f in fm if f.get("vcodec") == "none" and has(f, "acodec")]
        m4 = lambda s: est(pick_a([f for f in auds if f.get("ext") == "m4a"] or auds, s), dur)
        out = {"title": i.get("title") or "Video", "thumb": thumb(i), "dur": dur, "by": i.get("uploader") or i.get("channel"),
               "playlist": False, "count": 0, "video": V,
               "audio": [tile("m4a", "M4A", m4(True), m4(False)), tile("mp3", "MP3", int(dur * 16000), int(dur * 24000))]}
    INFO[url] = (time.time(), out)
    return out


# ---------- download ----------
def work(jid, url, r):
    J = JOBS[jid]
    d = J["dir"]
    d.mkdir(exist_ok=True)
    q, done = r.quality, [0]

    def hook(h):
        if J.get("cancel"):
            raise DownloadCancelled("cancelled")
        inf = h.get("info_dict") or {}
        if h["status"] == "finished":
            done[0] += 1
            return
        n = min(inf.get("n_entries") or 1, 10)
        per = len(inf.get("requested_formats") or [1])
        tot = h.get("total_bytes") or h.get("total_bytes_estimate") or 0
        frac = (h.get("downloaded_bytes") or 0) / tot if tot else 0
        J.update(status="downloading", percent=round(min(99, (done[0] + frac) / (per * n) * 100), 1),
                 speed=h.get("speed"), eta=h.get("eta"))

    def pp(h):
        if h["status"] == "started":
            J.update(status="processing", percent=99)

    opts = {**BASE, "noplaylist": not r.playlist, "playlistend": 10, "format": selector(q, r.saver),
            "outtmpl": str(d / "%(title).80B.%(ext)s"), "restrictfilenames": True,
            "max_filesize": MAX_MB * 1_000_000, "merge_output_format": "mp4",
            "match_filter": yt_dlp.utils.match_filter_func(f"duration <=? {MAX_MIN * 60} & !is_live"),
            "progress_hooks": [hook], "postprocessor_hooks": [pp], "concurrent_fragment_downloads": 4}
    if r.playlist:
        opts["ignoreerrors"] = "only_download"  # one bad video shouldn't kill the rest
    if q in ("mp3", "m4a"):
        opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": q, "preferredquality": "128" if r.saver else "192"}]
    try:
        with yt_dlp.YoutubeDL(opts) as y:
            info = y.extract_info(url) or {}
        files = sorted(p for p in d.iterdir() if p.is_file() and p.suffix not in (".part", ".ytdl", ".temp"))
        if not files:
            raise UserError(f"Nothing downloaded. Check the {MAX_MIN} min / {MAX_MB} MB limits.")
        if len(files) > 1:  # media is already compressed: store, don't deflate
            f = d / "playlist.zip"
            with zipfile.ZipFile(f, "w", zipfile.ZIP_STORED) as z:
                for p in files:
                    z.write(p, p.name)
            for p in files:
                p.unlink()
            name = "GrabIt-playlist.zip"
        else:
            f = files[0]
            name = safe(info.get("title") or "video") + f.suffix
        if f.stat().st_size > MAX_MB * 1_000_000:
            raise UserError(f"File is larger than the {MAX_MB} MB limit.")
        J.update(status="done", percent=100, file=str(f), name=name, bytes=f.stat().st_size, done_at=time.time())
    except DownloadCancelled:
        J.update(status="error", error="Cancelled.", done_at=time.time())
    except UserError as e:
        J.update(status="error", error=str(e), done_at=time.time())
    except Exception as e:
        log.warning("download failed: %s", e)
        J.update(status="error", error=friendly(e), done_at=time.time())


def run(jid, url, r):
    J = JOBS[jid]
    while not SLOT.acquire(timeout=1):  # queued behind other downloads
        if J.get("cancel"):
            return J.update(status="error", error="Cancelled.", done_at=time.time())
    try:
        J["status"] = "starting"
        work(jid, url, r)
    finally:
        SLOT.release()


async def janitor():
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for jid, J in list(JOBS.items()):
            if J.get("done_at") and now - J["done_at"] > TTL:
                shutil.rmtree(J["dir"], ignore_errors=True)
                JOBS.pop(jid, None)
                for k in [k for k, v in KEYS.items() if v == jid]:
                    KEYS.pop(k, None)
            elif not J.get("done_at") and now - J["t0"] > 7200:
                J["cancel"] = True  # stuck job watchdog
        for u in [u for u, (t, _) in INFO.items() if now - t > 300]:
            INFO.pop(u, None)
        for ip in list(HITS):
            if not HITS[ip] or HITS[ip][-1] < now - 60:
                HITS.pop(ip, None)


async def updater():
    while AUTO_UPDATE:  # daily yt-dlp upgrade; restart (when idle) to load it
        await asyncio.sleep(86400)
        before = yt_dlp.version.__version__
        pip = [sys.executable, "-m", "pip", "install", "--user", "-q", "-U", "yt-dlp[default]"]
        await asyncio.to_thread(subprocess.run, pip, capture_output=True)
        out = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", "import yt_dlp.version as v;print(v.__version__)"], capture_output=True, text=True)
        busy = any(not j.get("done_at") for j in JOBS.values())
        if out.stdout.strip() not in ("", before) and not busy:
            os.kill(os.getpid(), signal.SIGTERM)  # platform restarts the container


@asynccontextmanager
async def life(app):
    ts = [asyncio.create_task(janitor()), asyncio.create_task(updater())]
    yield
    for t in ts:
        t.cancel()


app = FastAPI(lifespan=life, docs_url=None, redoc_url=None)
CSP = "default-src 'self'; img-src 'self' data: https:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; frame-ancestors 'none'"


@app.middleware("http")
async def guard(request: Request, call_next):
    if request.method == "POST" and request.url.path in ("/api/info", "/api/download"):
        ip = request.client.host  # uvicorn --proxy-headers already resolves the real client; never trust raw X-Forwarded-For
        q, now = HITS[ip], time.time()
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= RATE:
            return JSONResponse({"detail": "Too many requests. Wait a minute."}, status_code=429)
        q.append(now)
    r = await call_next(request)
    r.headers.update({"X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer", "Content-Security-Policy": CSP})
    return r


@app.post("/api/info")
async def info(r: Req):
    try:
        return await asyncio.to_thread(get_info, r)
    except UserError as e:
        raise HTTPException(400, str(e))


@app.post("/api/download")
async def download(r: Req):
    if not re.fullmatch(r"v(2160|1440|1080|720|480|360|240)|mp3|m4a", r.quality):
        raise HTTPException(400, "Invalid quality.")
    try:
        url = await asyncio.to_thread(check_url, r.url)
    except UserError as e:
        raise HTTPException(400, str(e))
    key = (url, r.quality, r.playlist, r.saver)
    old = JOBS.get(KEYS.get(key))
    if old and old["status"] != "error":  # same request already running or finished: reuse, no second download
        return {"job_id": KEYS[key]}
    if sum(1 for j in JOBS.values() if not j.get("done_at")) >= 30:
        raise HTTPException(503, "Server is busy. Try again soon.")
    jid = uuid.uuid4().hex
    JOBS[jid] = {"status": "queued", "percent": 0, "dir": ROOT / jid, "t0": time.time()}
    KEYS[key] = jid
    threading.Thread(target=run, args=(jid, url, r), daemon=True).start()
    return {"job_id": jid}


@app.post("/api/cancel/{jid}")
def cancel(jid: str):
    if jid in JOBS:
        JOBS[jid]["cancel"] = True
    return {"ok": True}


@app.get("/api/progress/{jid}")
async def progress(jid: str):
    if jid not in JOBS:
        raise HTTPException(404, "Unknown job.")

    async def gen():
        last, idle = None, 0
        while True:
            J = JOBS.get(jid)
            if not J:
                yield 'data: {"status":"error","error":"Download expired."}\n\n'
                return
            d = {k: J.get(k) for k in ("status", "error", "name", "bytes", "speed", "eta")}
            d["percent"] = int(J.get("percent") or 0)
            if (d["status"], d["percent"]) != last:  # only send on change: less chatter on mobile data
                last, idle = (d["status"], d["percent"]), 0
                yield "data: " + json.dumps(d) + "\n\n"
            elif idle >= 20:
                idle = 0
                yield ": ping\n\n"
            if d["status"] in ("done", "error"):
                return
            idle += 1
            await asyncio.sleep(0.8)

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/file/{jid}")
def file(jid: str):
    J = JOBS.get(jid)
    if not J or J["status"] != "done":
        raise HTTPException(404, "File not ready or expired.")
    J["done_at"] = time.time()  # keep alive while it's being fetched; Range requests let a dropped download resume instead of restarting
    return FileResponse(J["file"], filename=J["name"])


@app.get("/health")
def health():
    return {"ok": True, "yt_dlp": yt_dlp.version.__version__}


def asset(name, ctype):  # gzip once at startup + ETag: repeat visits cost a ~200-byte 304
    raw = (HERE / name).read_bytes()
    gz = gzip.compress(raw, 9)
    tag = 'W/"%s"' % hashlib.md5(raw).hexdigest()[:16]
    head = {"ETag": tag, "Cache-Control": "no-cache", "Vary": "Accept-Encoding"}

    def h(request: Request):
        if tag in request.headers.get("if-none-match", ""):
            return Response(status_code=304, headers=head)
        if "gzip" in request.headers.get("accept-encoding", ""):
            return Response(gz, media_type=ctype, headers={**head, "Content-Encoding": "gzip"})
        return Response(raw, media_type=ctype, headers=head)
    return h


for p, n, t in (("/", "index.html", "text/html"), ("/sw.js", "sw.js", "text/javascript"),
                ("/manifest.webmanifest", "manifest.webmanifest", "application/manifest+json"),
                ("/icon.svg", "icon.svg", "image/svg+xml")):
    app.add_api_route(p, asset(n, t), include_in_schema=False)
