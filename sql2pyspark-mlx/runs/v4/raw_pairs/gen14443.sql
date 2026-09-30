WITH valid AS (
    SELECT
        id,
        store_id,
        quantity,
        CASE WHEN quantity >= 203 THEN 'High' WHEN quantity >= 84 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM inventory
    WHERE last_restock > TO_DATE('2024-03-01')
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity) AS quantity_dense_rank,
        LEAD(quantity, 2) OVER w_part AS next_quantity,
        SUM(quantity) OVER w_recent AS cumulative_quantity,
        LAST_VALUE(quantity) IGNORE NULLS OVER w_recent AS last_known_quantity,
        PERCENT_RANK() OVER w_hist AS quantity_percent_rank
    FROM valid
    WINDOW w_part AS (PARTITION BY store_id ORDER BY quantity, id), w_recent AS (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST)
),
with_dims AS (
    SELECT e.*, b.city
    FROM enriched e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
daily AS (
    SELECT
        city,
        COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows,
        MAX(last_known_quantity) AS max_last_known_quantity
    FROM with_dims
    GROUP BY CUBE (city)
)
SELECT
    city,
    chiang_mai_rows,
    max_last_known_quantity
FROM daily
ORDER BY city DESC
