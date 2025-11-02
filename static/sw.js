const CACHE_NAME = 'thumbnail-cache-v1';
const THUMB_URL_PATTERN = /^\/thumb\//;

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(clients.claim());
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);
  
  // 只缓存缩略图请求
  if (THUMB_URL_PATTERN.test(url.pathname)) {
    event.respondWith(
      caches.open(CACHE_NAME).then((cache) => {
        return cache.match(request).then((response) => {
          // 如果缓存中有响应，直接返回
          if (response) {
            return response;
          }
          
          // 否则发起网络请求，并将响应添加到缓存中
          return fetch(request).then((networkResponse) => {
            // 复制响应，因为流只能读取一次
            const responseToCache = networkResponse.clone();
            cache.put(request, responseToCache);
            return networkResponse;
          });
        });
      })
    );
  }
});