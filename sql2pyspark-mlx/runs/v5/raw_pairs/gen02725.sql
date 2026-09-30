WITH joined AS (
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.sale_date,
        src.revenue,
        p.product_name,
        p.cost,
        p.brand
    FROM store_sales src
    LEFT JOIN products p
        ON src.product_id = p.id
    WHERE ARRAY_CONTAINS(p.tags, 'sale')
),
cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        cost,
        LAST_DAY(sale_date) AS sale_date_month_end,
        CONCAT(product_name, ' - ', brand) AS product_name_brand
    FROM joined
    WHERE sale_date >= DATE '2023-05-17'
      AND brand IS NOT NULL
),
valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        cost,
        sale_date_month_end,
        product_name_brand,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM cleaned
    WHERE MONTH(sale_date_month_end) IN (2, 3, 9)
),
enriched AS (
    SELECT
        e.id,
        e.product_id,
        e.revenue,
        p.brand,
        p.category
    FROM valid e
    INNER JOIN products p
        ON e.product_id = p.id
),
joined_store_sales AS (
    SELECT
        src.*,
        b.price,
        b.stock_qty
    FROM enriched src
    LEFT JOIN products b
        ON src.product_id = b.id
),
matched_keys AS (
    SELECT id, product_id
    FROM joined_store_sales
    INTERSECT
    SELECT id, product_id
    FROM store_sales
    WHERE revenue >= 3200
),
verified AS (
    SELECT s.*
    FROM joined_store_sales s
    INNER JOIN matched_keys k ON s.id = k.id AND s.product_id = k.product_id
),
base AS (
    SELECT
        product_id,
        revenue,
        price,
        stock_qty,
        CASE WHEN brand = 'Globex' THEN 1 ELSE 0 END AS is_globex,
        COALESCE(brand, 'N/A') AS resolved_brand
    FROM verified
    WHERE category IS NOT DISTINCT FROM 'Home'
)
SELECT
    CASE WHEN is_globex >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_globex_tier,
    CASE WHEN stock_qty >= 449 THEN 'LARGE' ELSE 'SMALL' END AS stock_qty_tier,
    SUM(revenue) AS total_revenue,
    PERCENTILE_APPROX(stock_qty, 0.5) AS stock_qty_p50,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty
FROM base
GROUP BY CASE WHEN is_globex >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN stock_qty >= 449 THEN 'LARGE' ELSE 'SMALL' END;
