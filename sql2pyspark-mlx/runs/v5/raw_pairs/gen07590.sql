WITH latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.segment,
        s.signup_date,
        s.credit_limit,
        s.age,
        x.status
    FROM customers s
    JOIN LATERAL (
            SELECT c.status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(signup_date) AS signup_date_month_end,
        DATE_SUB(signup_date, 87) AS signup_date_minus_87d,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM latest_match
    WHERE segment IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        credit_limit,
        age,
        status_filled,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date_minus_87d, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY segment, status ORDER BY age NULLS LAST) AS age_cume_dist,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) AS credit_limit_share,
        FIRST_VALUE(age) IGNORE NULLS OVER w_grp AS first_known_age
    FROM base
    WHERE signup_date_minus_87d BETWEEN '2023-04-01' AND '2025-01-05'
    WINDOW w_grp AS (PARTITION BY status ORDER BY signup_date_minus_87d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, age_cume_dist * credit_limit_share AS age_cume_dist_times_credit_limit_share
    FROM scored
    WHERE age > 35
)
-- Grouped metrics
SELECT
    seq_no,
    CORR(credit_limit, age) AS corr_credit_limit_age,
    BIT_AND(age) AS age_bit_and,
    COUNT(*) AS row_count
FROM valid
GROUP BY seq_no
ORDER BY age_bit_and NULLS FIRST
