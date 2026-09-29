WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        DATE_TRUNC('month', sale_date) sale_date_period
    FROM store_sales
    WHERE sale_date > DATE '2023-08-14'
      AND store_id IN (1, 3)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        b.sqft,
        b.opened_date
    FROM filtered src
    LEFT JOIN stores b
        ON src.store_id = b.id
    WHERE b.city IN ('Bangkok', 'Berlin', 'New York')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY store_id, product_id ORDER BY sqft DESC NULLS FIRST) AS row_num
    FROM with_dims
    QUALIFY row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        sale_date,
        sale_date_period,
        sqft,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY sqft, id) sqft_bucket,
        LAST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_sqft
    FROM latest
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN sqft >= 14766 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT id) AS distinct_ids_2,
        MIN(sale_date_period) AS first_sale_date_period,
        MIN(sale_date_period) AS first_sale_date_period_2
    FROM enriched
    GROUP BY CASE WHEN sqft >= 14766 THEN 'HIGH' ELSE 'LOW' END
    HAVING distinct_ids_2 > 1
)
SELECT
    sqft_tier,
    distinct_ids,
    distinct_ids_2,
    first_sale_date_period,
    first_sale_date_period_2
FROM stats
ORDER BY distinct_ids_2 DESC;
