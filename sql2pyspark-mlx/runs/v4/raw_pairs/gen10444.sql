WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(category) AS category_length
    FROM products
    WHERE launch_date >= TO_DATE('2023-10-14')
      AND brand IS NOT NULL
      AND price IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY category, brand ORDER BY category_length DESC) AS category_length_percent_rank,
        LEAD(price, 2) OVER (PARTITION BY category ORDER BY category_length ASC, id ASC) AS next_price
    FROM base
),
enriched_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(price / SUM(price) OVER (PARTITION BY brand), 4) AS price_share,
        FIRST_VALUE(category_length, TRUE) OVER (ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_category_length,
        LAG(price, 1, 0) OVER (PARTITION BY brand ORDER BY cost ASC, id ASC) AS prev_price
    FROM enriched
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        category_length,
        category_length_percent_rank,
        next_price,
        price_share,
        first_known_category_length,
        prev_price,
        LEAD(price_share, 1) OVER (PARTITION BY supplier_id ORDER BY category_length_percent_rank ASC, id ASC) AS next_price_share,
        SUM(category_length_percent_rank) OVER w_ordered AS cumulative_category_length_percent_rank,
        CUME_DIST() OVER (PARTITION BY brand, category ORDER BY category_length_percent_rank ASC NULLS LAST) AS category_length_percent_rank_cume_dist
    FROM enriched_products
    WINDOW w_ordered AS (PARTITION BY supplier_id ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.quantity = 5
        )
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    CASE WHEN brand RLIKE '^(Globex|Stark)$' THEN 'GROUP_B' WHEN brand RLIKE '^(Acme|Initech)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS brand_zone
FROM kept
ORDER BY brand DESC, category ASC;
