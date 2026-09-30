WITH latest AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units NULLS LAST, id) = 1
),
base AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        CAST(units AS STRING) AS units_str
    FROM latest
    WHERE YEAR(sale_date) >= 2025
),
enriched AS (
    SELECT a.*, d.store_name
    FROM base a
    INNER JOIN stores d
        ON a.store_id = d.id
),
with_dims AS (
    SELECT
        s.*,
        p.sqft,
        p.opened_date
    FROM enriched s
    LEFT JOIN stores p
        ON s.store_id = p.id
)
SELECT
    CASE WHEN sqft >= 14265 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    BIT_OR(units) AS units_bit_or,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    BIT_OR(units) AS units_bit_or_2,
    PERCENTILE_APPROX(sqft, 0.5) AS sqft_p50
FROM with_dims
GROUP BY 1, 2;
