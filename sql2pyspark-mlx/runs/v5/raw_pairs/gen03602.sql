WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) AS rank_in_group
        FROM sessions
    ) t
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.rank_in_group,
    (SELECT SUM(x.amount) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_amount
FROM deduped s
ORDER BY customer_id
