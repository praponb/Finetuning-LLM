WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        UPPER(email) AS email_upper,
        COALESCE(city, 'OTHER') AS city_clean
    FROM customers
    WHERE signup_date >= DATE '2023-05-14'
      AND city IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(credit_limit) OVER w_grp AS next_credit_limit,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM filtered
    WHERE segment IN ('SMB', 'Consumer', 'Corporate')
    WINDOW w_grp AS (PARTITION BY segment, city ORDER BY credit_limit ASC, id ASC)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(row_seq) OVER (PARTITION BY city, segment ORDER BY next_credit_limit ASC, id ASC) AS next_row_seq,
        LAG(row_seq) OVER (PARTITION BY city, segment ORDER BY next_credit_limit ASC, id ASC) AS prev_row_seq,
        LAG(next_credit_limit) OVER (PARTITION BY city, segment ORDER BY next_credit_limit ASC, id ASC) AS prev_next_credit_limit,
        LEAD(next_credit_limit) OVER (PARTITION BY city, segment ORDER BY next_credit_limit ASC, id ASC) AS next_next_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY next_credit_limit ASC, id ASC) AS row_seq_2,
        LAG(credit_limit, 2) OVER (PARTITION BY city, segment ORDER BY row_seq ASC, id ASC) AS prev_credit_limit
    FROM with_windows
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        city,
        segment,
        is_active,
        credit_limit,
        email_upper,
        next_row_seq,
        prev_row_seq,
        prev_next_credit_limit,
        prev_credit_limit,
        CASE WHEN prev_credit_limit >= 13400 THEN 'High' WHEN prev_credit_limit >= 4900 THEN 'Medium' ELSE 'Low' END AS prev_credit_limit_level,
        ROUND(prev_credit_limit / prev_row_seq, 2) AS prev_credit_limit_per_prev_row_seq,
        COALESCE(segment, 'UNKNOWN') AS segment_clean
    FROM metrics
    WHERE signup_date >= TO_DATE('2023-05-08')
      AND next_row_seq < 6
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY prev_next_credit_limit DESC, id ASC) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
)
-- Grouped metrics
SELECT
    CASE WHEN prev_credit_limit_per_prev_row_seq >= 60 THEN 'LARGE' ELSE 'SMALL' END AS prev_credit_limit_per_prev_row_seq_tier,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    AVG(credit_limit) FILTER (WHERE city = 'London') AS london_avg_credit_limit,
    MAX(credit_limit) AS max_credit_limit,
    ROUND(SUM(prev_credit_limit) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS prev_credit_limit_per_london,
    ROUND(AVG(prev_credit_limit), 2) AS avg_prev_credit_limit
FROM latest
GROUP BY prev_credit_limit_per_prev_row_seq_tier;
