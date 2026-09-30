WITH valid AS (
    SELECT *, ARRAY_CONTAINS(tags, 'eco') AS is_eco
    FROM products
    WHERE launch_date >= TO_DATE('2023-06-07')
      AND ARRAY_CONTAINS(tags, 'eco')
),
enriched AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        tags,
        CORR(cost, stock_qty) OVER (PARTITION BY brand) AS corr_cost_stock_qty,
        ROUND(price / SUM(price) OVER (PARTITION BY supplier_id), 4) AS price_share,
        LAG(cost, 1) OVER (PARTITION BY brand ORDER BY cost, id) AS prev_cost
    FROM valid
),
cleaned AS (
    SELECT
        id,
        product_name,
        brand,
        cost,
        tags,
        price_share,
        prev_cost,
        LENGTH(category) AS category_length,
        SUBSTRING(category, 1, 4) AS category_prefix
    FROM enriched
    WHERE brand IS NOT DISTINCT FROM 'Stark'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY category_length DESC, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    product_name,
    brand,
    cost,
    tags,
    price_share,
    category_prefix,
    COUNT(category_length) OVER (PARTITION BY brand ORDER BY rank_in_group, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_category_length,
    MIN(category_length) OVER (PARTITION BY brand ORDER BY rank_in_group, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_category_length,
    MAX(category_length) OVER (PARTITION BY brand ORDER BY rank_in_group, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_category_length,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY prev_cost DESC, id) AS row_seq
FROM ranked;
