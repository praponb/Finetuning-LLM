WITH matched_keys AS (
    -- Set difference on keys
    SELECT region
    FROM stores
    EXCEPT ALL
    SELECT region
    FROM stores
    WHERE region != 'Central'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM stores s
    JOIN matched_keys k ON s.region = k.region
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    APPROX_PERCENTILE(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    BIT_AND(sqft) AS sqft_bit_and
FROM kept_rows
GROUP BY 1
ORDER BY week_start DESC
