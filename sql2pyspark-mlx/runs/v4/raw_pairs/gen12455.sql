WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        SORT_ARRAY(tags) AS sorted_tags
    FROM products
    WHERE brand = 'Initech'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(cost, TRUE) OVER (PARTITION BY category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        PERCENT_RANK() OVER (PARTITION BY category, brand ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_percent_rank
    FROM filtered
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        first_known_cost,
        COALESCE(brand, 'N/A') AS brand_filled,
        COALESCE(brand, 'NONE') AS brand_clean
    FROM enriched
    WHERE brand IS NOT NULL
)
-- Ranking and gap features
SELECT
    id,
    category,
    brand,
    price,
    cost,
    stock_qty,
    tags,
    first_known_cost,
    brand_filled,
    brand_clean,
    SUM(first_known_cost) OVER w_recent AS running_first_known_cost,
    SUM(stock_qty) OVER w_recent AS cumulative_stock_qty,
    AVG(cost) OVER (PARTITION BY brand, category) AS brand_avg_cost,
    FIRST_VALUE(stock_qty) IGNORE NULLS OVER (PARTITION BY brand ORDER BY first_known_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_stock_qty
FROM cleaned
WINDOW w_recent AS (PARTITION BY category ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
