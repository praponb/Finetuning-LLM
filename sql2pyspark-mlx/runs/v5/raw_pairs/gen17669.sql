WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        COALESCE(sqft, 0) AS sqft_filled
    FROM stores
    WHERE opened_date > DATE '2023-05-05'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM base
    INTERSECT ALL
    SELECT id, city
    FROM stores
    WHERE sqft <> 6170
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST) AS sqft_rank
    FROM reconciled
    WHERE city = 'Berlin'
),
with_windows_stores AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        sqft_filled,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft DESC NULLS LAST) AS sqft_percent_rank,
        COUNT(sqft_rank) OVER (PARTITION BY city) AS city_count_sqft_rank,
        ROUND(sqft_rank / SUM(sqft_rank) OVER (PARTITION BY city), 4) AS sqft_rank_share
    FROM with_windows
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(sqft_percent_rank * 1.15, 2) AS sqft_percent_rank_scaled,
        sqft_filled * sqft_percent_rank AS sqft_filled_times_sqft_percent_rank
    FROM with_windows_stores
    WHERE sqft_rank_share IS NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM cleaned
    EXCEPT
    SELECT id
    FROM stores
    WHERE MONTH(opened_date) IN (9, 10, 11)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    store_name,
    sqft_filled,
    sqft_percent_rank,
    city_count_sqft_rank,
    sqft_rank_share,
    sqft_percent_rank_scaled,
    sqft_filled_times_sqft_percent_rank
FROM kept_rows;
