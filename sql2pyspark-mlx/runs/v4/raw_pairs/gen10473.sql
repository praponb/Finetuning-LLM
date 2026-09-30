SELECT method, INITCAP(TRIM(status)) AS status_title
FROM payments
WHERE method IS NOT NULL
ORDER BY status_title DESC
