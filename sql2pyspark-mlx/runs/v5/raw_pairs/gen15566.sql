WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE sale_date >= DATE '2023-08-27'
      AND revenue <= 1500
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        NTILE(4) OVER (PARTITION BY store_id, product_id ORDER BY units ASC, id ASC) AS units_bucket,
        SUM(revenue_int) OVER (PARTITION BY product_id ORDER BY units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue_int
    FROM base
    WHERE product_id IN (2, 5, 9)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        s.city
    FROM with_windows e
    INNER JOIN stores s
        ON e.store_id = s.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        d.sqft,
        d.city AS store_city
    FROM enriched src
    INNER JOIN stores d
        ON src.store_id = d.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN sqft >= 12241 THEN 'High' WHEN sqft >= 8199 THEN 'Medium' ELSE 'Low' END AS sqft_level
    FROM with_dims
    WHERE store_id IN (1, 2, 5, 6)
)
-- Join a dimension and aggregate
SELECT
    d.city AS store_city,
    d.region,
    a.city AS store_sale_city,
    COUNT(*) AS num_rows,
    ROUND(SUM(a.sqft), 2) AS total_sqft
FROM cleaned a
LEFT JOIN stores d ON a.store_id = d.id
GROUP BY CUBE (d.city, d.region, a.city);
