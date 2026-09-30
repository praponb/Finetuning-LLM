WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        SUBSTRING(category, 1, 4) AS category_prefix
    FROM products
    WHERE supplier_id IS NOT NULL
      AND cost BETWEEN 260 AND 470
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(price, 2) OVER (PARTITION BY brand ORDER BY price ASC, id ASC) AS prev_price,
        NTILE(4) OVER (PARTITION BY brand ORDER BY price ASC, id ASC) AS price_bucket,
        LEAD(cost, 2, 0) OVER (PARTITION BY brand, category ORDER BY launch_date ASC, id ASC) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost ASC, id ASC) AS seq_no_2
    FROM cleaned
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category, brand ORDER BY next_cost DESC NULLS FIRST, id ASC) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ARRAY_CONTAINS(tags, 'sale') AS is_sale,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM latest
    WHERE price >= 860
)
SELECT
    id,
    brand,
    cost,
    category_prefix,
    price_bucket,
    next_cost,
    latest_rank,
    is_sale,
    brand_clean
FROM filtered
ORDER BY next_cost DESC NULLS LAST, brand DESC
