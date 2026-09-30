WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        DATE_SUB(opened_date, 52) AS opened_date_minus_52d,
        DAYOFWEEK(opened_date) AS opened_date_dayofweek,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
      AND sqft BETWEEN 6811 AND 8475
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(sqft, 1) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft,
        NTILE(4) OVER (ORDER BY opened_date_dayofweek DESC, id) AS opened_date_dayofweek_bucket
    FROM cleaned
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM scored
    INTERSECT ALL
    SELECT id
    FROM stores
    WHERE region NOT IN ('West', 'Central')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id
)
-- Window metrics over several different partitions
SELECT
    *,
    CUME_DIST() OVER (ORDER BY sqft DESC NULLS LAST) AS sqft_cume_dist,
    ROUND(prev_sqft / SUM(prev_sqft) OVER (PARTITION BY city), 4) AS prev_sqft_share,
    RANK() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_rank
FROM kept_rows;
