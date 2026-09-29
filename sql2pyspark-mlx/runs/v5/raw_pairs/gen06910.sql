SELECT id, status, opened_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) pos
    FROM accounts
) t
WHERE pos <= 5
ORDER BY opened_date, id
