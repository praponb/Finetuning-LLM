WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN sqft >= 13576 THEN 'High' WHEN sqft >= 5247 THEN 'Medium' ELSE 'Low' END sqft_level,
        COALESCE(region, 'OTHER') region_clean
    FROM stores
    WHERE opened_date >= DATE '2023-04-13'
      AND region IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        sqft_level,
        region_clean,
        INITCAP(TRIM(region)) region_title
    FROM cleaned
    WHERE opened_date > DATE '2023-08-02'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM valid
    INTERSECT
    SELECT id, city
    FROM stores
    WHERE region = 'West'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        opened_date,
        sqft,
        region_title,
        DENSE_RANK() OVER w_recent sqft_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft NULLS FIRST) sqft_percent_rank,
        MIN(sqft) OVER (PARTITION BY city) city_min_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) sqft_share
    FROM verified
    WINDOW w_recent AS (ORDER BY sqft NULLS LAST)
),
cleaned_stores AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        opened_date,
        sqft,
        region_title,
        sqft_dense_rank,
        sqft_percent_rank,
        city_min_sqft,
        sqft_share,
        LAST_DAY(opened_date) opened_date_month_end
    FROM metrics
    WHERE opened_date >= DATE '2023-09-04'
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    city,
    metric,
    reading
FROM (
    SELECT
        city,
        sqft_percent_rank,
        sqft_share
    FROM cleaned_stores
) src
UNPIVOT (
    reading FOR metric IN (sqft_percent_rank AS SQFT_PERCENT_RANK_VAL, sqft_share)
) long_rows;
