WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(revenue * 1.15, 2) revenue_adjusted
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-09-23')
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY store_id, product_id ORDER BY sale_date NULLS LAST) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_adjusted,
        latest_rank,
        SUM(revenue) OVER (PARTITION BY store_id ORDER BY revenue_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_revenue,
        AVG(units) OVER (ORDER BY revenue_adjusted, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) units_avg_2_prior,
        AVG(revenue_adjusted) OVER (PARTITION BY store_id ORDER BY revenue_adjusted, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) revenue_adjusted_avg_5,
        SUM(revenue_adjusted) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_revenue_adjusted,
        LAG(revenue_adjusted, 2) OVER (ORDER BY sale_date, id) prev_revenue_adjusted
    FROM deduped
),
with_dims AS (
    -- Attach stores attributes
    SELECT src.*, b.opened_date
    FROM scored src
    JOIN stores b
        ON src.store_id = b.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.opened_date AS store_opened_date,
        b.sqft
    FROM with_dims e
    JOIN stores b
        ON e.store_id = b.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC NULLS LAST, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    store_id,
    COUNT(*) AS cnt,
    MAX(revenue_adjusted_avg_5) AS max_revenue_adjusted_avg_5,
    COUNT(*) AS event_count,
    MAX(revenue) AS max_revenue,
    ROUND(AVG(prev_revenue_adjusted), 2) AS avg_prev_revenue_adjusted,
    COUNT(DISTINCT product_id) AS distinct_product_ids
FROM ranked
GROUP BY store_id;
