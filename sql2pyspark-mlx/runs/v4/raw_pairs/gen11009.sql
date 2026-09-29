WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('WEEK', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock >= DATE '2024-04-23'
)
-- Ranking and gap features
SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    last_restock_period,
    ROW_NUMBER() OVER w_ordered AS seq_no,
    SUM(quantity) OVER (ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS quantity_sum_30d,
    AVG(quantity) OVER (ORDER BY CAST(last_restock_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS quantity_avg_90d
FROM filtered
WINDOW w_ordered AS (PARTITION BY product_id ORDER BY last_restock_period DESC, id);
