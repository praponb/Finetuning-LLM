WITH joined AS (
    SELECT e.*, b.brand
    FROM order_items e
    LEFT JOIN products b
        ON e.product_id <=> b.id
),
enriched AS (
    SELECT
        *,
        MIN(quantity) OVER (PARTITION BY order_id) AS order_id_min_quantity,
        SUM(unit_price) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price
    FROM joined
),
metrics AS (
    SELECT
        id,
        order_id,
        quantity,
        brand,
        cumulative_unit_price,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY cumulative_unit_price DESC, id) AS cumulative_unit_price_bucket,
        FIRST_VALUE(cumulative_unit_price) IGNORE NULLS OVER (PARTITION BY brand ORDER BY order_id_min_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_unit_price,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price_2
    FROM enriched
    WHERE brand IS NOT NULL
),
enriched_order_items AS (
    SELECT
        *,
        LEAD(cumulative_unit_price_2, 2, 0) OVER (PARTITION BY order_id ORDER BY first_known_cumulative_unit_price, id) AS next_cumulative_unit_price_2,
        NTILE(4) OVER (PARTITION BY order_id, brand ORDER BY cumulative_unit_price_2, id) AS cumulative_unit_price_2_bucket,
        AVG(quantity) OVER (PARTITION BY order_id, brand ORDER BY cumulative_unit_price_bucket, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5,
        SUM(quantity) OVER (PARTITION BY brand ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cumulative_unit_price, id) AS seq_no
    FROM metrics
),
scored AS (
    SELECT
        *,
        ROUND(first_known_cumulative_unit_price / SUM(first_known_cumulative_unit_price) OVER (PARTITION BY order_id, brand), 4) AS first_known_cumulative_unit_price_share,
        CUME_DIST() OVER (PARTITION BY brand ORDER BY cumulative_unit_price_2_bucket) AS cumulative_unit_price_2_bucket_cume_dist
    FROM enriched_order_items
)
SELECT
    CASE WHEN cumulative_unit_price >= 2243 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_unit_price_tier,
    order_id,
    CASE WHEN quantity >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
    AVG(cumulative_unit_price_2) AS avg_cumulative_unit_price_2,
    ROUND(AVG(cumulative_unit_price_2_bucket), 2) AS avg_cumulative_unit_price_2_bucket
FROM scored
GROUP BY cumulative_unit_price_tier, order_id, quantity_tier
