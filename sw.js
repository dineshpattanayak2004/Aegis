self.addEventListener('push', event => {
  let data = { title: 'AEGIS alert', detail: 'Open AEGIS for details', level: 'warn' };
  try { data = { ...data, ...event.data.json() }; } catch (e) {}
  event.waitUntil(self.registration.showNotification(data.title, {
    body: data.detail,
    tag: data.title,
    requireInteraction: data.level === 'emerg' || data.level === 'warn',
    vibrate: [300, 150, 300]
  }));
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(clients.openWindow('/'));
});