WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAY(opened_date) AS opened_date_day,
        LAST_DAY(opened_date) AS opened_date_month_end,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM stores
    WHERE opened_date >= TO_DATE('2023-10-20')
      AND region IS NOT NULL
)
-- Ranking and gap features
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    opened_date_day,
    opened_date_month_end,
    region_clean,
    LAG(sqft, 1) OVER (PARTITION BY region, city ORDER BY opened_date_day, id) AS prev_sqft,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
    FIRST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY opened_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region
FROM filtered;
