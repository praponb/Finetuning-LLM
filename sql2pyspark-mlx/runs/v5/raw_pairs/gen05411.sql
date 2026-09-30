WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(unit_price) AS unit_price_whole
    FROM order_items
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.quantity,
        a.unit_price,
        a.unit_price_whole,
        d.brand,
        d.category
    FROM filtered a
    LEFT JOIN products d
        ON a.product_id = d.id
    WHERE d.stock_qty < 183
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY quantity ASC NULLS LAST, id ASC) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        brand,
        SUM(unit_price) OVER (PARTITION BY brand) AS brand_sum_unit_price,
        LEAD(unit_price, 1) OVER (ORDER BY unit_price ASC, id ASC) AS next_unit_price,
        NTILE(4) OVER (PARTITION BY category, brand ORDER BY unit_price DESC, id ASC) AS unit_price_bucket
    FROM latest
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity ASC, id ASC) AS row_num
    FROM metrics
    QUALIFY row_num = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(row_num / unit_price_bucket, 2) AS row_num_per_unit_price_bucket,
        brand_sum_unit_price * unit_price_bucket AS brand_sum_unit_price_times_unit_price_bucket
    FROM deduped
    WHERE brand IS NOT NULL
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.price,
        p.launch_date
    FROM cleaned a
    INNER JOIN products p
        ON a.product_id = p.id
)
-- Multi-level aggregation
SELECT
    product_id,
    CASE WHEN row_num_per_unit_price_bucket >= 77 THEN 'HIGH' ELSE 'LOW' END AS row_num_per_unit_price_bucket_tier,
    COUNT_IF(brand = 'Umbrella') AS umbrella_events,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    PERCENTILE_APPROX(unit_price_bucket, ARRAY(0.25, 0.5, 0.75)) AS unit_price_bucket_quartiles,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SIZE(COLLECT_SET(brand)) AS num_brands_2
FROM enriched
GROUP BY ROLLUP (product_id, CASE WHEN row_num_per_unit_price_bucket >= 77 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY product_id DESC;
