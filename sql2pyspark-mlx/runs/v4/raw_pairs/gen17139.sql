WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        segment,
        credit_limit,
        age,
        UPPER(segment) AS segment_upper,
        COALESCE(segment, 'DIRECT') AS segment_clean
    FROM customers
    WHERE segment IS NOT NULL
      AND country != 'SG'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age, id) AS row_num
        FROM latest
    ) x
    WHERE row_num = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country), 4) AS credit_limit_share,
        CUME_DIST() OVER (ORDER BY credit_limit NULLS LAST) AS credit_limit_cume_dist,
        CUME_DIST() OVER (PARTITION BY city ORDER BY credit_limit DESC NULLS LAST) AS credit_limit_cume_dist_2,
        NTILE(3) OVER (PARTITION BY city ORDER BY age DESC, id) AS age_bucket
    FROM deduped
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        country,
        age
    FROM with_windows
) src
PIVOT (
    SUM(age) AS amount
    FOR country IN ('TH' AS th, 'DE' AS de)
)
