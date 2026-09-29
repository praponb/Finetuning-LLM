WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age
    FROM customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY signup_date, id) = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.start_date > '2024-10-19'
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC NULLS LAST, id) AS row_num
        FROM kept
    ) t
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        segment,
        signup_date,
        age,
        LEAD(credit_limit) OVER (PARTITION BY segment, city ORDER BY signup_date, id) next_credit_limit,
        LEAD(age) OVER (PARTITION BY segment, city ORDER BY signup_date, id) next_age,
        ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY signup_date, id) row_seq,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment, city), 4) credit_limit_share
    FROM ranked
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY next_credit_limit DESC NULLS LAST, id) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    CASE WHEN age >= 62 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    SUM(next_credit_limit) AS total_next_credit_limit,
    SUM(CASE WHEN segment = 'SMB' THEN next_credit_limit ELSE 0 END) AS smb_next_credit_limit,
    COUNT(*) AS cnt,
    SUM(row_seq) FILTER (WHERE segment = 'SMB') AS smb_total_row_seq,
    COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY SUM(CASE WHEN segment = 'SMB' THEN next_credit_limit ELSE 0 END) DESC) AS smb_next_credit_limit_dense_rank
FROM deduped
GROUP BY DATE_TRUNC('MONTH', signup_date), CASE WHEN age >= 62 THEN 'HIGH' ELSE 'LOW' END
ORDER BY total_next_credit_limit DESC;
