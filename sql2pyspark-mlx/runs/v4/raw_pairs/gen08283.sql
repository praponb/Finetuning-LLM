WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE store_id IN (1, 3, 5, 6)
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.opened_date,
        d.sqft
    FROM base s
    INNER JOIN stores d
        ON s.store_id = d.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.category,
        p.product_name,
        p.launch_date
    FROM joined e
    INNER JOIN products p
        ON e.product_id = p.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        s.city,
        s.opened_date AS store_opened_date
    FROM enriched src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id, category), 4) AS units_share,
        DENSE_RANK() OVER (PARTITION BY city, category ORDER BY revenue DESC) AS revenue_dense_rank
    FROM with_dims
)
-- Aggregate with conditional metrics
SELECT
    city,
    CASE WHEN sqft >= 14748 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    BIT_OR(units) AS units_bit_or,
    ROUND(AVG(sqft), 2) AS avg_sqft
FROM metrics
GROUP BY city, sqft_tier, quarter_start
ORDER BY sqft_tier DESC
