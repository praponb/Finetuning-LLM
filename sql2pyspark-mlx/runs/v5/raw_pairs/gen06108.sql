WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM store_sales
    EXCEPT
    SELECT id
    FROM store_sales
    WHERE store_id IN (2, 3, 4, 5)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN remaining_keys k ON s.id = k.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        sale_date,
        units,
        revenue,
        RANK() OVER (ORDER BY units DESC NULLS FIRST) AS units_rank,
        ROUND(units / SUM(units) OVER (PARTITION BY product_id, store_id), 4) AS units_share,
        MIN(units) OVER (PARTITION BY store_id) AS store_id_min_units
    FROM kept_rows
),
metrics_store_sales AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY revenue DESC NULLS FIRST) AS revenue_cume_dist,
        RANK() OVER (ORDER BY units ASC NULLS LAST) AS units_rank_2,
        RANK() OVER (ORDER BY store_id_min_units DESC NULLS FIRST) AS store_id_min_units_rank,
        RANK() OVER (ORDER BY units_share ASC NULLS LAST) AS units_share_rank
    FROM metrics
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        sale_date,
        units,
        revenue,
        units_rank,
        units_share,
        revenue_cume_dist,
        units_rank_2,
        store_id_min_units_rank,
        units_share_rank,
        units_share - store_id_min_units_rank AS units_share_minus_store_id_min_units_rank,
        ROUND(units_share * 1.15, 2) AS units_share_scaled
    FROM metrics_store_sales
    WHERE sale_date >= DATE '2023-08-22'
      AND sale_date BETWEEN '2023-04-01' AND '2024-04-01'
)
-- Grouped metrics
SELECT
    CASE WHEN units_share_scaled >= 71 THEN 'LARGE' ELSE 'SMALL' END AS units_share_scaled_tier,
    MIN(units) AS min_units,
    ROUND(AVG(store_id_min_units_rank), 2) AS avg_store_id_min_units_rank,
    ROUND(AVG(store_id_min_units_rank), 2) AS avg_store_id_min_units_rank_2
FROM cleaned
GROUP BY CASE WHEN units_share_scaled >= 71 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(AVG(store_id_min_units_rank), 2) < 1000000
ORDER BY avg_store_id_min_units_rank_2 ASC;
