WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date_2,
        COALESCE(region, 'UNKNOWN') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
      AND region IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    store_name,
    region,
    sqft,
    days_since_opened_date,
    region_filled,
    AVG(days_since_opened_date) OVER (PARTITION BY region ORDER BY days_since_opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_opened_date_avg_6_prior,
    PERCENT_RANK() OVER w_recent AS days_since_opened_date_2_percent_rank,
    LAST_VALUE(days_since_opened_date) IGNORE NULLS OVER (PARTITION BY city, region ORDER BY days_since_opened_date_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_opened_date,
    RANK() OVER (ORDER BY days_since_opened_date_2 DESC NULLS FIRST) AS days_since_opened_date_2_rank
FROM cleaned
WINDOW w_recent AS (PARTITION BY region, city ORDER BY days_since_opened_date_2 DESC NULLS LAST)
