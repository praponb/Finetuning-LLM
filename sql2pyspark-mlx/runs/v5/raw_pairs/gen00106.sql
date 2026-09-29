WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id) pos
    FROM accounts
    WHERE status = 'OPEN'
)
SELECT *
FROM ranked
WHERE pos <= 1
ORDER BY 5 DESC, balance DESC;
