const C='grabit-v4',SHELL=['/','/manifest.webmanifest','/icon.svg'];
self.addEventListener('install',e=>e.waitUntil(caches.open(C).then(c=>c.addAll(SHELL)).then(()=>self.skipWaiting())));
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!==C).map(x=>caches.delete(x)))).then(()=>clients.claim())));
// Instant from cache, refreshed quietly in the background (server answers 304, so revalidation costs ~nothing).
self.addEventListener('fetch',e=>{const r=e.request,u=new URL(r.url);
 if(r.method!=='GET'||u.origin!==location.origin||u.pathname.startsWith('/api/')||u.pathname==='/health')return;
 const key=r.mode==='navigate'?'/':r;
 e.respondWith(caches.match(key).then(hit=>{
  const net=fetch(r.mode==='navigate'?'/':r).then(res=>{if(res.ok)caches.open(C).then(c=>c.put(key,res.clone()));return res}).catch(()=>hit);
  e.waitUntil(net.catch(()=>{}));return hit||net}))});
