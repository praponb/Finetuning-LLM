SELECT id, device
FROM sessions
WHERE country = 'US'
ORDER BY device DESC, id DESC
