WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DATE_TRUNC('YEAR', sale_date) AS sale_date_period
    FROM store_sales
    WHERE units IS NULL
)
SELECT
    DATE_TRUNC('YEAR', sale_date_period) AS year_start,
    CASE WHEN units >= 37 THEN 'HIGH' ELSE 'LOW' END AS units_tier,
    BIT_OR(units) AS units_bit_or,
    MIN(sale_date_period) AS first_seen,
    MAX(sale_date_period) AS last_seen,
    RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', sale_date_period) ORDER BY BIT_OR(units) DESC) AS units_bit_or_rank
FROM cleaned
GROUP BY DATE_TRUNC('YEAR', sale_date_period), CASE WHEN units >= 37 THEN 'HIGH' ELSE 'LOW' END
ORDER BY units_tier DESC;
