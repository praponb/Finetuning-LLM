WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN country IN ('TH', 'US') THEN 'Core' ELSE 'Other' END country_group,
        COALESCE(segment, 'DIRECT') segment_clean
    FROM customers
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Singapore'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(is_active AS INT) is_active_flag,
        TRIM(country) clean_country
    FROM valid
    WHERE signup_date > DATE '2023-06-18'
      AND segment IS NOT DISTINCT FROM 'Corporate'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(is_active_flag) OVER (PARTITION BY city ORDER BY credit_limit ASC, id ASC) prev_is_active_flag,
        LAG(credit_limit) OVER (PARTITION BY city ORDER BY credit_limit ASC, id ASC) prev_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit ASC, id ASC) seq_no,
        AVG(credit_limit) OVER (ORDER BY signup_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) credit_limit_avg_3
    FROM cleaned
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY prev_credit_limit DESC NULLS FIRST, id ASC) AS dedup_rank
        FROM with_windows
    ) x
    WHERE dedup_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city,
        segment,
        signup_date,
        country_group,
        clean_country,
        prev_credit_limit,
        dedup_rank
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY age DESC) = 1
)
-- Grouped metrics
SELECT
    city,
    segment,
    ROUND(SUM(prev_credit_limit) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS prev_credit_limit_per_singapore,
    MAX(prev_credit_limit) AS max_prev_credit_limit
FROM deduped
GROUP BY 1, 2
ORDER BY prev_credit_limit_per_singapore ASC NULLS LAST;
