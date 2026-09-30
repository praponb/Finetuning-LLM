WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date,
        COALESCE(region, 'NONE') region_clean
    FROM stores
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'South'
)
SELECT
    id,
    city,
    sqft,
    days_since_opened_date,
    region_clean,
    SUM(sqft) OVER w_ordered running_sum_sqft,
    COUNT(sqft) OVER w_ordered running_count_sqft,
    MIN(sqft) OVER w_ordered running_min_sqft,
    SUM(sqft) OVER w_ordered_2 cumulative_sqft
FROM cleaned
WINDOW w_ordered AS (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered_2 AS (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
