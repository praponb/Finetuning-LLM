SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    region,
    COUNT(*) AS row_count,
    BIT_OR(sqft) AS sqft_bit_or,
    COUNT(*) FILTER (WHERE region = 'West') AS west_rows
FROM stores
GROUP BY 1, 2
HAVING COUNT(*) < 1
