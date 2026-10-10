import asyncio, ipaddress, json, logging, os, re, shutil, signal, socket, subprocess, sys, tempfile, threading, time, uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

import yt_dlp
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

MAX_MB = int(os.getenv("MAX_FILE_MB", 500))
MAX_MIN = int(os.getenv("MAX_DURATION_MIN", 60))
RATE = int(os.getenv("RATE_LIMIT", 5))  # POST requests per minute per IP
TTL = 600  # delete finished files after 10 min
HERE = Path(__file__).parent
ROOT = Path(tempfile.gettempdir()) / "grabit"
ROOT.mkdir(exist_ok=True)
JOBS: dict = {}
HITS: dict = defaultdict(deque)
log = logging.getLogger("grabit")
BASE = dict(quiet=True, no_warnings=True, socket_timeout=15, retries=2)


class Req(BaseModel):
    url: str
    quality: str = "v720"
    playlist: bool = False


class UserError(Exception):
    pass


ERRORS = [
    (("drm",), "This video is DRM-protected. GrabIt can't download it."),
    (("unsupported url",), "This site isn't supported."),
    (("confirm your age", "age-restricted", "age restricted"), "This video is age-restricted and can't be downloaded."),
    (("not available in your country", "geo", "blocked it in your"), "This video is geo-blocked in the server's region."),
    (("not a bot",), "The site blocked this server (bot check). Try another video or host."),
    (("private",), "This video is private."),
    (("unavailable", "removed", "does not exist", "404"), "This video was removed or is unavailable."),
]


def friendly(e):
    m = str(e).lower()
    for keys, msg in ERRORS:
        if any(k in m for k in keys):
            return msg
    return "Couldn't process that link. Check it and try again."


def check_url(u):
    u = u.strip()
    p = urlparse(u)
    if p.scheme not in ("http", "https") or not p.hostname:
        raise UserError("Please enter a valid http(s) link.")
    try:
        for i in socket.getaddrinfo(p.hostname, p.port or 80):
            if not ipaddress.ip_address(i[4][0]).is_global:
                raise UserError("That address isn't allowed.")
    except (socket.gaierror, ValueError):
        raise UserError("Couldn't find that website.")
    return u


def safe(t):
    return re.sub(r'[\\/:*?"<>|\x00-\x1f]', "", t).strip(" .")[:100] or "video"


def it(id_, label, size=0):
    return {"id": id_, "label": label, "size": size, "disabled": size > MAX_MB * 1_000_000}


def get_info(r):
    url = check_url(r.url)
    try:
        with yt_dlp.YoutubeDL({**BASE, "noplaylist": not r.playlist, "extract_flat": "in_playlist", "playlistend": 10}) as y:
            i = y.extract_info(url, download=False)
    except Exception as e:
        log.warning("info failed: %s", e)
        raise UserError(friendly(e))
    if i.get("_type") == "playlist":
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
    size = lambda f: (f.get("filesize") or f.get("filesize_approx") or 0) if f else 0
    vids = [f for f in ok if f.get("vcodec") not in (None, "none") and f.get("height")]
    auds = [f for f in ok if f.get("vcodec") == "none" and f.get("acodec") not in (None, "none")]
    ab = max(auds, key=lambda f: f.get("abr") or 0, default=None)
    m4 = max((f for f in auds if f.get("ext") == "m4a"), key=lambda f: f.get("abr") or 0, default=None)
    top = max((f["height"] for f in vids), default=0)
    V = []
    for h in (2160, 1440, 1080, 720, 480, 360):
        c = [f for f in vids if f["height"] <= h]
        if top >= h and c:
            b = max(c, key=lambda f: (f["height"], f.get("tbr") or 0))
            extra = 0 if b.get("acodec") not in (None, "none") else size(ab)
            V.append(it(f"v{h}", f"{h}p", size(b) + extra))
    groups = [
        {"name": "Video", "items": V},
        {"name": "Audio only", "items": [it("mp3", "MP3 192k", int(dur * 24000)), it("m4a", "M4A", size(m4) or size(ab))]},
        {"name": "Best available", "items": [it("best", "Best", V[0]["size"] if V else size(ab))]},
    ]
    return {"title": i.get("title") or "Video", "thumbnail": i.get("thumbnail"), "duration": dur,
            "uploader": i.get("uploader") or i.get("channel"), "playlist_count": 0, "groups": groups}


def run(jid, url, r):
    J = JOBS[jid]
    d = J["dir"]
    d.mkdir(exist_ok=True)
    q = r.quality
    done = [0]

    def hook(h):
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

    fmt = {"best": "bv*+ba/b", "mp3": "bestaudio/best", "m4a": "bestaudio[ext=m4a]/bestaudio/best"}.get(
        q) or f"bv*[height<={q[1:]}]+ba/b[height<={q[1:]}]"
    opts = {**BASE, "noplaylist": not r.playlist, "playlistend": 10, "format": fmt,
            "outtmpl": str(d / "%(title).80B.%(ext)s"), "restrictfilenames": True,
            "max_filesize": MAX_MB * 1_000_000, "merge_output_format": "mp4",
            "match_filter": yt_dlp.utils.match_filter_func(f"duration <=? {MAX_MIN * 60} & !is_live"),
            "progress_hooks": [hook], "postprocessor_hooks": [pp], "concurrent_fragment_downloads": 4}
    if q in ("mp3", "m4a"):
        opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": q, "preferredquality": "192"}]
    try:
        with yt_dlp.YoutubeDL(opts) as y:
            info = y.extract_info(url)
        files = [p for p in d.iterdir() if p.is_file() and p.suffix not in (".part", ".ytdl", ".temp")]
        if not files:
            raise UserError(f"Nothing downloaded. The video may be longer than {MAX_MIN} min or larger than {MAX_MB} MB.")
        if len(files) > 1:
            z = shutil.make_archive(str(ROOT / jid), "zip", d)
            for p in files:
                p.unlink()
            f = d / "playlist.zip"
            shutil.move(z, f)
            name = "GrabIt-playlist.zip"
        else:
            f = files[0]
            name = safe(info.get("title") or "video") + f.suffix
        if f.stat().st_size > MAX_MB * 1_000_000:
            raise UserError(f"File is larger than the {MAX_MB} MB limit.")
        J.update(status="done", percent=100, file=str(f), name=name, done_at=time.time())
    except UserError as e:
        J.update(status="error", error=str(e), done_at=time.time())
    except Exception as e:
        log.warning("download failed: %s", e)
        J.update(status="error", error=friendly(e), done_at=time.time())


async def janitor():
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for jid, J in list(JOBS.items()):
            if J.get("done_at") and now - J["done_at"] > TTL:
                shutil.rmtree(J["dir"], ignore_errors=True)
                JOBS.pop(jid, None)
        for ip in list(HITS):
            if not HITS[ip] or HITS[ip][-1] < now - 60:
                HITS.pop(ip, None)


async def updater():
    while True:  # daily yt-dlp upgrade; restart (when idle) to load it
        await asyncio.sleep(86400)
        before = yt_dlp.version.__version__
        await asyncio.to_thread(subprocess.run, [sys.executable, "-m", "pip", "install", "-U", "yt-dlp"], capture_output=True)
        out = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", "import yt_dlp.version as v;print(v.__version__)"], capture_output=True, text=True)
        busy = any(j["status"] in ("starting", "downloading", "processing") for j in JOBS.values())
        if out.stdout.strip() not in ("", before) and not busy:
            os.kill(os.getpid(), signal.SIGTERM)  # platform restarts the container


@asynccontextmanager
async def life(app):
    ts = [asyncio.create_task(janitor()), asyncio.create_task(updater())]
    yield
    for t in ts:
        t.cancel()


app = FastAPI(lifespan=life, docs_url=None, redoc_url=None)


@app.middleware("http")
async def limiter(request: Request, call_next):
    if request.method == "POST" and request.url.path.startswith("/api/"):
        ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or request.client.host
        q, now = HITS[ip], time.time()
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= RATE:
            return JSONResponse({"detail": "Too many requests. Wait a minute and try again."}, status_code=429)
        q.append(now)
    return await call_next(request)


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
    jid = uuid.uuid4().hex
    JOBS[jid] = {"status": "starting", "percent": 0, "dir": ROOT / jid}
    threading.Thread(target=run, args=(jid, url, r), daemon=True).start()
    return {"job_id": jid}


@app.get("/api/progress/{jid}")
async def progress(jid: str):
    if jid not in JOBS:
        raise HTTPException(404, "Unknown job.")

    async def gen():
        while True:
            J = JOBS.get(jid)
            if not J:
                yield 'data: {"status":"error","error":"Job expired."}\n\n'
                return
            yield "data: " + json.dumps({k: J.get(k) for k in ("status", "percent", "speed", "eta", "error")}) + "\n\n"
            if J["status"] in ("done", "error"):
                return
            await asyncio.sleep(0.7)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/file/{jid}")
def file(jid: str):
    J = JOBS.get(jid)
    if not J or J["status"] != "done":
        raise HTTPException(404, "File not ready or expired.")
    return FileResponse(J["file"], filename=J["name"])


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
