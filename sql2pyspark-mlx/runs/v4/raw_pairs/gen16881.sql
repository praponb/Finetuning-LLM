WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(units, revenue) AS least_units_revenue
    FROM store_sales
    WHERE units >= 20
)
-- Ranking and gap features
SELECT
    *,
    MIN(least_units_revenue) OVER (PARTITION BY product_id, store_id) AS product_id_min_least_units_revenue,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY least_units_revenue, id) AS row_seq,
    LAG(revenue, 1) OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS prev_revenue,
    FIRST_VALUE(least_units_revenue) IGNORE NULLS OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_least_units_revenue
FROM filtered
