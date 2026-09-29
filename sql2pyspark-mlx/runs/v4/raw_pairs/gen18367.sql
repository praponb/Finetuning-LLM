WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST, id) AS rank_in_group
    FROM inventory
    QUALIFY rank_in_group = 1
)
SELECT
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    AVG(quantity) AS avg_quantity,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows,
    COUNT(*) AS cnt
FROM ranked
GROUP BY activity_month
