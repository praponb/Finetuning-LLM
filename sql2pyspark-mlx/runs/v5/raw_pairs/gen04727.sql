WITH latest AS (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY last_restock DESC) AS row_num
    FROM inventory
    QUALIFY row_num = 1
)
SELECT
    store_id,
    APPROX_PERCENTILE(quantity, 0.9) AS quantity_p90,
    SUM(quantity) AS total_quantity
FROM latest
GROUP BY store_id
