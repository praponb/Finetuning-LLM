WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        city,
        segment,
        CAST(is_active AS INT) AS is_active_flag,
        COALESCE(segment, 'OTHER') AS segment_filled
    FROM customers
    WHERE name LIKE 'Jac%'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY is_active_flag DESC NULLS LAST, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(is_active_flag) OVER (PARTITION BY segment) AS segment_count_is_active_flag,
        LEAD(is_active_flag, 1) OVER w_running AS next_is_active_flag,
        FIRST_VALUE(is_active_flag) IGNORE NULLS OVER (PARTITION BY city ORDER BY is_active_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_active_flag
    FROM latest
    WINDOW w_running AS (PARTITION BY segment, city ORDER BY is_active_flag, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        is_active_flag,
        next_is_active_flag,
        first_known_is_active_flag,
        AVG(first_known_is_active_flag) OVER (PARTITION BY segment, city) AS segment_avg_first_known_is_active_flag,
        AVG(is_active_flag) OVER (PARTITION BY city ORDER BY first_known_is_active_flag, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS is_active_flag_avg_5_prior,
        SUM(next_is_active_flag) OVER (ORDER BY first_known_is_active_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_is_active_flag,
        NTILE(3) OVER (ORDER BY is_active_flag, id) AS is_active_flag_bucket,
        AVG(is_active_flag) OVER (ORDER BY is_active_flag, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS is_active_flag_avg_5_prior_2
    FROM metrics
)
SELECT
    id,
    email,
    is_active_flag,
    first_known_is_active_flag,
    segment_avg_first_known_is_active_flag,
    is_active_flag_avg_5_prior_2
FROM enriched
ORDER BY is_active_flag DESC
