WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN city IN ('Berlin', 'London') THEN 'Tier 1' ELSE 'Other' END AS city_group
    FROM customers
    WHERE signup_date > DATE '2024-01-10'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY age, id) AS row_seq,
        NTILE(4) OVER (PARTITION BY city ORDER BY age, id) AS age_bucket,
        LAG(age, 2) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS prev_age
    FROM base
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY prev_age DESC NULLS LAST, id) AS dedup_rank
        FROM scored
    ) x
    WHERE dedup_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC NULLS LAST, id) = 1
)
-- Window metrics over several different partitions
SELECT
    *,
    ROUND(prev_age / SUM(prev_age) OVER (PARTITION BY country), 4) AS prev_age_share,
    LAST_VALUE(row_seq, TRUE) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY row_seq DESC, id) AS seq_no,
    LAST_VALUE(credit_limit, TRUE) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit
FROM ranked;
