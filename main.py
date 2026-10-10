import asyncio, ipaddress, logging, os, re, shutil, socket, subprocess, sys, tempfile, threading, time, uuid, zipfile
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from email.utils import formatdate
from pathlib import Path
from urllib.parse import quote, urlparse

import yt_dlp
import yt_dlp.version
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

MAX_MB = int(os.getenv("MAX_FILE_MB", 500))
MAX_MIN = int(os.getenv("MAX_DURATION_MIN", 60))
RATE = int(os.getenv("RATE_LIMIT", 5))  # POST requests per minute per IP
MAX_JOBS = int(os.getenv("MAX_JOBS", 2))  # downloads running at the same time (phone-friendly)
MIN_FREE_MB = 300  # refuse new downloads when the phone has less free space
TTL = 600  # delete finished files 10 min after their last use
ABANDON = 600  # stop a running job if the page stopped checking in for 10 min
HERE = Path(__file__).parent
ROOT = Path(tempfile.gettempdir()) / "grabit"
shutil.rmtree(ROOT, ignore_errors=True)  # leftovers from a previous run eat phone storage
ROOT.mkdir(parents=True, exist_ok=True)
JOBS: dict = {}
HITS: dict = defaultdict(deque)
SLOTS = threading.BoundedSemaphore(MAX_JOBS)
log = logging.getLogger("grabit")
BASE = dict(quiet=True, no_warnings=True, socket_timeout=15, retries=2)
MIME = {".mp4": "video/mp4", ".webm": "video/webm", ".mkv": "video/x-matroska", ".mp3": "audio/mpeg",
        ".m4a": "audio/mp4", ".zip": "application/zip"}


class Req(BaseModel):
    url: str = Field(max_length=2048)
    quality: str = Field("v720", max_length=10)
    playlist: bool = False


class UserError(Exception):
    pass


class Stop(yt_dlp.utils.DownloadCancelled):
    """Raised inside yt-dlp's progress hook to stop a job (cancel, too big, abandoned)."""


ERRORS = [
    (("drm",), "This video is DRM-protected. GrabIt can't download it."),
    (("unsupported url",), "This site isn't supported."),
    (("confirm your age", "age-restricted", "age restricted"), "This video is age-restricted and can't be downloaded."),
    (("not available in your country", "geo-restrict", "geo restrict", "blocked it in your"),
     "This video is geo-blocked in the server's region."),
    (("not a bot",), "The site blocked this server (bot check). Try another video or host."),
    (("sign in", "log in", "login required"), "This video needs a login, so GrabIt can't download it."),
    (("private video", "is private"), "This video is private."),
    (("requested format is not available",), "That quality isn't available for this video. Pick another one."),
    (("ffmpeg", "ffprobe"), "ffmpeg is missing or failed. In Termux run: pkg install ffmpeg"),
    (("no space left",), "The phone is out of storage. Free some space and try again."),
    (("http error 429", "too many requests"), "The site is rate-limiting this server. Wait a few minutes and try again."),
    (("timed out", "timeout", "urlopen error", "connection reset", "network is unreachable"),
     "Network problem while reaching the site. Check the phone's internet and try again."),
    (("unavailable", "removed", "does not exist", "404"), "This video was removed or is unavailable."),
]


def detail(e):
    """Short, readable yt-dlp error text. Links are masked so they never reach logs or the UI."""
    s = re.sub(r"\x1b\[[0-9;]*m", "", str(e)).strip()
    s = re.sub(r"^(ERROR:\s*)+", "", s)
    s = re.sub(r"https?://\S+", "<link>", s)
    return s[:200]


def friendly(e):
    m = str(e).lower()
    for keys, msg in ERRORS:
        if any(k in m for k in keys):
            return msg
    d = detail(e)
    return f"Couldn't process that link: {d}" if d else "Couldn't process that link. Check it and try again."


def check_url(u):
    u = u.strip()
    p = urlparse(u)
    if p.scheme not in ("http", "https") or not p.hostname:
        raise UserError("Please enter a valid http(s) link.")
    try:
        port = p.port
    except ValueError:
        raise UserError("That link has an invalid port number.")
    try:
        for i in socket.getaddrinfo(p.hostname, port or 80):
            if not ipaddress.ip_address(i[4][0]).is_global:
                raise UserError("That address isn't allowed.")
    except (socket.gaierror, ValueError):
        raise UserError("Couldn't find that website.")
    return u


def safe(t):
    t = re.sub(r'[\\/:*?"<>|\x00-\x1f\u200e\u200f\u202a-\u202e\u2066-\u2069]', "", t).strip(" .")
    t = t.encode("utf-8")[:120].decode("utf-8", "ignore")  # Android allows 255 bytes; Arabic/emoji use 2-4 per char
    return t.strip(" .") or "video"


def it(id_, label, size=0):
    return {"id": id_, "label": label, "size": size, "disabled": size > MAX_MB * 1_000_000}


def est(f, dur):
    if not f:
        return 0
    s = f.get("filesize") or f.get("filesize_approx") or 0
    if not s and dur and f.get("tbr"):
        s = f["tbr"] * 125 * dur  # kbit/s -> bytes
    return int(s)


def get_info(r):
    url = check_url(r.url)
    try:
        with yt_dlp.YoutubeDL({**BASE, "noplaylist": not r.playlist, "extract_flat": "in_playlist", "playlistend": 10}) as y:
            i = y.extract_info(url, download=False)
    except Exception as e:
        log.warning("info failed (%s)", type(e).__name__)
        raise UserError(friendly(e))
    if not i:
        raise UserError("Nothing was found at that link.")
    if i.get("_type") in ("playlist", "multi_video"):
        if not r.playlist:
            raise UserError("This link is a playlist. Tick “Download playlist” and analyze again.")
        es = [e for e in (i.get("entries") or []) if e][:10]
        thumb = next((e["thumbnails"][-1]["url"] for e in es if e.get("thumbnails")), None)
        groups = [
            {"name": "Video", "items": [it(f"v{h}", f"{h}p") for h in (1080, 720, 480, 360)]},
            {"name": "Audio only", "items": [it("mp3", "MP3"), it("m4a", "M4A")]},
            {"name": "Best available", "items": [it("best", "Best")]},
        ]
        return {"title": i.get("title") or "Playlist", "thumbnail": thumb, "duration": 0,
                "uploader": i.get("uploader"), "playlist_count": len(es), "groups": groups}
    if i.get("is_live"):
        raise UserError("Live streams aren't supported.")
    dur = i.get("duration") or 0
    if dur > MAX_MIN * 60:
        raise UserError(f"This video is longer than the {MAX_MIN}-minute limit.")
    fm = i.get("formats") or []
    ok = [f for f in fm if f.get("has_drm") is not True]
    if fm and not ok:
        raise UserError(friendly("drm"))
    vids = [f for f in ok if f.get("vcodec") not in (None, "none") and f.get("height")]
    auds = [f for f in ok if f.get("vcodec") == "none" and f.get("acodec") not in (None, "none")]
    ab = max(auds, key=lambda f: f.get("abr") or 0, default=None)
    m4 = max((f for f in auds if f.get("ext") == "m4a"), key=lambda f: f.get("abr") or 0, default=None)
    top = max((f["height"] for f in vids), default=0)
    V = []
    for h in (2160, 1440, 1080, 720, 480, 360, 240, 144):
        c = [f for f in vids if f["height"] <= h]
        if top >= h and c:
            b = max(c, key=lambda f: (f["height"], f.get("tbr") or 0))
            extra = 0 if b.get("acodec") not in (None, "none") else est(ab, dur)
            V.append(it(f"v{h}", f"{h}p", est(b, dur) + extra))
    groups = [
        {"name": "Video", "items": V},
        {"name": "Audio only", "items": [it("mp3", "MP3 192k", int(dur * 24000)), it("m4a", "M4A", est(m4, dur) or est(ab, dur))]},
        {"name": "Best available", "items": [it("best", "Best", V[0]["size"] if V else est(ab, dur))]},
    ]
    return {"title": i.get("title") or "Video", "thumbnail": i.get("thumbnail"), "duration": dur,
            "uploader": i.get("uploader") or i.get("channel"), "playlist_count": 0, "groups": groups}


def run(jid, url, r):
    J = JOBS[jid]
    d = J["dir"]
    q = r.quality
    limit = MAX_MB * 1_000_000
    done = [0]  # finished streams
    base = [0]  # bytes of finished streams
    got_slot = False

    def stop(msg):
        J["abort"] = J.get("abort") or msg
        raise Stop()

    def hook(h):
        if J.get("cancel"):
            stop("Cancelled.")
        got = h.get("downloaded_bytes") or 0
        if h["status"] == "finished":
            done[0] += 1
            base[0] += got
            return
        if base[0] + got > limit:
            stop(f"This download would be larger than the {MAX_MB} MB limit.")
        inf = h.get("info_dict") or {}
        n = min(inf.get("n_entries") or 1, 10)
        per = len(inf.get("requested_formats") or [1])
        tot = h.get("total_bytes") or h.get("total_bytes_estimate") or 0
        if tot:
            frac = got / tot
        elif h.get("fragment_count"):
            frac = (h.get("fragment_index") or 0) / h["fragment_count"]
        else:
            frac = 0
        pct = min(99, (done[0] + min(frac, 1)) / (per * n) * 100)
        J.update(status="downloading", percent=round(max(J.get("percent") or 0, pct), 1),
                 speed=h.get("speed"), eta=h.get("eta"))

    def pp(h):
        if h["status"] == "started":
            J.update(status="processing", percent=99)

    # H.264/AAC is preferred inside the same resolution so the MP4 plays on every phone.
    fmt = {"best": "bv*+ba/b", "mp3": "bestaudio/best", "m4a": "bestaudio[ext=m4a]/bestaudio/best"}.get(
        q) or f"bv*[height<=?{q[1:]}]+ba/b[height<=?{q[1:]}]"
    opts = {**BASE, "noplaylist": not r.playlist, "playlistend": 10, "format": fmt,
            "format_sort": ["res", "vcodec:h264", "acodec:m4a"],
            "outtmpl": str(d / "%(title).60B-%(id)s.%(ext)s"), "restrictfilenames": True,
            "max_filesize": limit, "merge_output_format": "mp4",
            "match_filter": yt_dlp.utils.match_filter_func(f"duration <=? {MAX_MIN * 60} & !is_live"),
            "progress_hooks": [hook], "postprocessor_hooks": [pp], "concurrent_fragment_downloads": 4}
    if q in ("mp3", "m4a"):
        opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": q, "preferredquality": "192"}]
    try:
        while not got_slot:  # wait for a free download slot
            got_slot = SLOTS.acquire(timeout=1)
            if J.get("cancel"):
                stop("Cancelled.")
        d.mkdir(exist_ok=True)
        J.update(status="downloading")
        with yt_dlp.YoutubeDL(opts) as y:
            info = y.extract_info(url) or {}
        files = sorted(p for p in d.iterdir() if p.is_file() and not re.search(r"\.(part(-Frag\d+)?|ytdl|temp)$", p.name))
        if not files:
            raise UserError(f"Nothing downloaded. The video may be longer than {MAX_MIN} min or larger than {MAX_MB} MB.")
        if len(files) > 1:
            f = d / "playlist.zip"
            with zipfile.ZipFile(f, "w", zipfile.ZIP_STORED, allowZip64=True) as zf:  # stored: no CPU/battery wasted
                for p in files:
                    zf.write(p, p.name)
            for p in files:
                p.unlink()
            name = "GrabIt-playlist.zip"
        else:
            f = files[0]
            name = f.name if info.get("_type") == "playlist" else safe(info.get("title") or "video") + f.suffix
        size = f.stat().st_size
        if size > limit:
            raise UserError(f"File is larger than the {MAX_MB} MB limit.")
        now = time.time()
        J.update(status="done", percent=100, file=str(f), name=name, size=size, done_at=now, touch=now)
    except UserError as e:
        J.update(status="error", error=str(e), done_at=time.time())
    except Exception as e:
        if J.get("abort"):
            J.update(status="error", error=J["abort"], done_at=time.time())
        else:
            log.warning("download failed (%s)", type(e).__name__)
            J.update(status="error", error=friendly(e), done_at=time.time())
    finally:
        if got_slot:
            SLOTS.release()


async def janitor():
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for jid, J in list(JOBS.items()):
            if J["status"] in ("done", "error"):
                if now - max(J.get("done_at") or 0, J.get("touch") or 0) > TTL:
                    await asyncio.to_thread(shutil.rmtree, J["dir"], True)
                    JOBS.pop(jid, None)
            elif now - (J.get("touch") or now) > ABANDON and not J.get("cancel"):
                J["abort"] = "Stopped because the app was closed for 10 minutes. Tap Try again."
                J["cancel"] = True
        for ip in list(HITS):
            if not HITS[ip] or HITS[ip][-1] < now - 60:
                HITS.pop(ip, None)


async def updater():
    while True:  # daily yt-dlp upgrade; restart (when idle) to load it
        await asyncio.sleep(86400)
        before = yt_dlp.version.__version__
        await asyncio.to_thread(subprocess.run, [sys.executable, "-m", "pip", "install", "-U", "yt-dlp"], capture_output=True)
        out = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", "import yt_dlp.version as v;print(v.__version__)"], capture_output=True, text=True)
        busy = any(j["status"] in ("queued", "downloading", "processing") for j in JOBS.values())
        if out.stdout.strip() not in ("", before) and not busy:
            try:  # re-run the same command in place (works in Termux and Docker; no supervisor needed)
                os.execv(sys.executable, [sys.executable, *sys.orig_argv[1:]])
            except Exception as e:
                log.warning("restart after upgrade failed (%s); restart GrabIt by hand", type(e).__name__)


@asynccontextmanager
async def life(app):
    ts = [asyncio.create_task(janitor()), asyncio.create_task(updater())]
    yield
    for t in ts:
        t.cancel()


app = FastAPI(lifespan=life, docs_url=None, redoc_url=None)


class Guard:
    """Rate limit for POST /api/* plus basic security headers. Plain ASGI, so file streaming stays fast."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        path = scope["path"]
        if scope["method"] == "POST" and path.startswith("/api/") and not path.startswith("/api/cancel/"):
            h = {k.decode("latin-1"): v.decode("latin-1") for k, v in scope["headers"]}
            ip = h.get("cf-connecting-ip") or (scope.get("client") or ("?",))[0]
            q, now = HITS[ip], time.time()
            while q and now - q[0] > 60:
                q.popleft()
            if len(q) >= RATE:
                wait = int(60 - (now - q[0])) + 1
                res = JSONResponse({"detail": f"Too many requests. Wait {wait} seconds and try again."}, status_code=429)
                return await res(scope, receive, send)
            q.append(now)

        async def snd(m):
            if m["type"] == "http.response.start":
                m["headers"] = [*m.get("headers", []), (b"x-content-type-options", b"nosniff"),
                                (b"referrer-policy", b"no-referrer")]
            await send(m)

        await self.app(scope, receive, snd)


app.add_middleware(Guard)


@app.post("/api/info")
async def info(r: Req):
    try:
        return await asyncio.to_thread(get_info, r)
    except UserError as e:
        raise HTTPException(400, str(e))


@app.post("/api/download")
async def download(r: Req):
    if not re.fullmatch(r"v\d{3,4}|best|mp3|m4a", r.quality):
        raise HTTPException(400, "Invalid quality.")
    try:
        url = await asyncio.to_thread(check_url, r.url)
    except UserError as e:
        raise HTTPException(400, str(e))
    if sum(1 for j in JOBS.values() if j["status"] in ("queued", "downloading", "processing")) >= 8:
        raise HTTPException(429, "GrabIt is busy with other downloads. Try again in a minute.")
    free = shutil.disk_usage(ROOT).free // 1_000_000
    if free < MIN_FREE_MB:
        raise HTTPException(507, f"The phone is almost out of storage ({free} MB free). Free some space and try again.")
    jid = uuid.uuid4().hex
    JOBS[jid] = {"status": "queued", "percent": 0, "dir": ROOT / jid, "touch": time.time()}
    threading.Thread(target=run, args=(jid, url, r), daemon=True).start()
    return {"job_id": jid}


@app.get("/api/status/{jid}")
async def status(jid: str):
    J = JOBS.get(jid)
    if not J:
        raise HTTPException(404, "This download expired or the server restarted.")
    J["touch"] = time.time()
    out = {k: J.get(k) for k in ("status", "percent", "speed", "eta", "error", "name", "size")}
    return JSONResponse(out, headers={"Cache-Control": "no-store"})


@app.post("/api/cancel/{jid}")
async def cancel(jid: str):
    J = JOBS.get(jid)
    if J and J["status"] not in ("done", "error"):
        J["abort"] = "Cancelled."
        J["cancel"] = True
    return {"ok": True}


@app.api_route("/api/file/{jid}", methods=["GET", "HEAD"])
def file(jid: str, request: Request):
    J = JOBS.get(jid)
    if not J or J["status"] != "done":
        return PlainTextResponse("This file expired (files are deleted 10 minutes after last use). Go back to GrabIt and download it again.",
                                 status_code=404, headers={"Content-Disposition": "inline"})
    p = Path(J["file"])
    try:
        st = p.stat()
    except OSError:
        return PlainTextResponse("This file is gone. Go back to GrabIt and download it again.",
                                 status_code=404, headers={"Content-Disposition": "inline"})
    J["touch"] = time.time()
    size, name = st.st_size, J["name"]
    etag = f'"{jid[:12]}-{size}-{int(st.st_mtime)}"'
    lastmod = formatdate(st.st_mtime, usegmt=True)

    # HTTP Range support: lets Android's download manager pause/resume instead of failing.
    start, end, code = 0, size - 1, 200
    rng, ifr = request.headers.get("range", "").strip(), request.headers.get("if-range")
    m = re.fullmatch(r"bytes=(\d*)-(\d*)", rng) if rng and (not ifr or ifr in (etag, lastmod)) else None
    if m and (m[1] or m[2]):
        if m[1]:
            start = int(m[1])
            end = min(int(m[2]), size - 1) if m[2] else size - 1
        else:
            start, end = max(0, size - int(m[2])), size - 1
        if start >= size or start > end:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
        code = 206
    length = end - start + 1

    ext = Path(name).suffix
    plain = re.sub(r"[^A-Za-z0-9._ -]", "", Path(name).stem.encode("ascii", "ignore").decode()).strip() or "video"
    headers = {
        "Content-Length": str(length), "Accept-Ranges": "bytes", "ETag": etag, "Last-Modified": lastmod,
        "Cache-Control": "no-store",
        "Content-Disposition": f"attachment; filename=\"{plain}{ext}\"; filename*=UTF-8''{quote(name, safe='')}",
    }
    if code == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    mime = MIME.get(ext.lower(), "application/octet-stream")
    if request.method == "HEAD":
        return Response(status_code=code, headers={**headers, "Content-Type": mime})

    def chunks():
        with open(p, "rb") as fh:
            fh.seek(start)
            left = length
            while left > 0:
                b = fh.read(min(262144, left))
                if not b:
                    break
                left -= len(b)
                J["touch"] = time.time()  # keeps the file alive while it is being downloaded
                yield b

    return StreamingResponse(chunks(), status_code=code, media_type=mime, headers=headers)


@app.get("/health")
def health():
    return {"ok": True, "yt_dlp": yt_dlp.version.__version__}


WEB = {"/": ("index.html", "text/html"), "/sw.js": ("sw.js", "text/javascript"),
       "/manifest.webmanifest": ("manifest.webmanifest", "application/manifest+json"),
       "/icon.svg": ("icon.svg", "image/svg+xml")}


def page(n, t):
    return lambda: FileResponse(HERE / n, media_type=t, headers={"Cache-Control": "no-cache"})


for p, (n, t) in WEB.items():
    app.add_api_route(p, page(n, t), include_in_schema=False)
