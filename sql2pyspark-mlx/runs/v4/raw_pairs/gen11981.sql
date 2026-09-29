WITH cleaned AS (
    SELECT
        *,
        ROUND(quantity * 1.15, 2) AS quantity_scaled,
        CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-05-16')
      AND quantity >= 113
),
with_dims AS (
    SELECT
        s.*,
        p.price,
        p.product_name
    FROM cleaned s
    LEFT JOIN products p
        ON s.product_id = p.id
),
filtered AS (
    SELECT *, COALESCE(product_name, 'N/A') AS product_name_filled
    FROM with_dims
    WHERE quantity < 215
),
valid AS (
    SELECT
        *,
        ROUND(price * 0.9, 2) AS adjusted_price,
        GREATEST(quantity_whole, price) AS greatest_quantity_whole_price
    FROM filtered
    WHERE last_restock >= DATE '2023-09-15'
),
grouped AS (
    SELECT
        CASE WHEN adjusted_price >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_price_tier,
        store_id,
        DATE_TRUNC('YEAR', last_restock) AS year_start,
        APPROX_PERCENTILE(greatest_quantity_whole_price, ARRAY(0.25, 0.5, 0.75)) AS greatest_quantity_whole_price_quartiles,
        ROUND(AVG(price), 2) AS avg_price,
        PERCENTILE_APPROX(price, ARRAY(0.25, 0.5, 0.75)) AS price_quartiles,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN adjusted_price >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY ROUND(AVG(price), 2) DESC) AS avg_price_percent_rank
    FROM valid
    GROUP BY CASE WHEN adjusted_price >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END, store_id, DATE_TRUNC('YEAR', last_restock)
)
SELECT
    adjusted_price_tier,
    store_id,
    year_start,
    greatest_quantity_whole_price_quartiles,
    avg_price,
    price_quartiles,
    row_count,
    quantity_p90,
    avg_price_percent_rank
FROM grouped
ORDER BY store_id DESC NULLS FIRST;
