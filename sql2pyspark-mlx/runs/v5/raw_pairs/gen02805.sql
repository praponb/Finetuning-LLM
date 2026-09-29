WITH dim_totals AS (
    SELECT
        p.channel,
        src.method,
        COUNT(*) AS cnt,
        MAX(src.amount) AS max_amount
    FROM payments src
    INNER JOIN orders p ON src.order_id = p.id
    GROUP BY p.channel, src.method
)
SELECT
    channel,
    method,
    cnt
FROM dim_totals
ORDER BY cnt NULLS LAST
