WITH scored AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY age DESC, id) AS age_bucket,
        LAG(age, 1, 0) OVER (PARTITION BY country ORDER BY signup_date, id) AS prev_age,
        LEAD(credit_limit, 1) OVER (PARTITION BY city ORDER BY credit_limit, id) AS next_credit_limit
    FROM customers
),
base AS (
    SELECT *, TRIM(segment) AS clean_segment
    FROM scored
    WHERE signup_date > DATE '2023-09-27'
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY city ORDER BY prev_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit,
        AVG(age_bucket) OVER (PARTITION BY city ORDER BY prev_age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS age_bucket_avg_6,
        FIRST_VALUE(age, TRUE) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age,
        LAG(prev_age, 1) OVER (ORDER BY age, id) AS prev_prev_age,
        SUM(next_credit_limit) OVER (ORDER BY prev_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_credit_limit
    FROM base
),
metrics AS (
    SELECT
        *,
        LAG(prev_prev_age, 1) OVER w_part AS prev_prev_prev_age,
        RANK() OVER (PARTITION BY segment ORDER BY prev_age DESC NULLS LAST) AS prev_age_rank,
        ROUND(prev_prev_age / SUM(prev_prev_age) OVER (PARTITION BY country), 4) AS prev_prev_age_share
    FROM with_windows
    WINDOW w_part AS (PARTITION BY segment ORDER BY age_bucket, id)
),
with_last AS (
    SELECT
        s.*,
        x.status,
        x.balance
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.status, c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows_customers AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY next_credit_limit DESC NULLS LAST) AS next_credit_limit_dense_rank,
        NTILE(5) OVER w_ordered AS prev_prev_age_bucket,
        AVG(age_bucket_avg_6) OVER w_grp AS age_bucket_avg_6_avg_3,
        COUNT(age_bucket_avg_6) OVER w_hist AS segment_count_age_bucket_avg_6,
        MIN(balance) OVER w_hist_2 AS city_min_balance
    FROM with_last
    WINDOW w_ordered AS (PARTITION BY segment ORDER BY prev_prev_age, id), w_grp AS (PARTITION BY city ORDER BY first_known_age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY segment), w_hist_2 AS (PARTITION BY city, segment)
),
base_customers AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        age,
        age_bucket,
        prev_age,
        next_credit_limit,
        clean_segment,
        last_known_credit_limit,
        age_bucket_avg_6,
        prev_prev_age,
        cumulative_next_credit_limit,
        prev_prev_prev_age,
        prev_age_rank,
        prev_prev_age_share,
        status,
        balance,
        next_credit_limit_dense_rank,
        prev_prev_age_bucket,
        age_bucket_avg_6_avg_3,
        segment_count_age_bucket_avg_6,
        ROUND(first_known_age, 1) AS first_known_age_rounded,
        ROUND(next_credit_limit_dense_rank * 0.9, 2) AS next_credit_limit_dense_rank_adjusted,
        COALESCE(city, 'NONE') AS resolved_city
    FROM with_windows_customers
    WHERE signup_date >= DATE '2023-09-15'
      AND status = 'FROZEN'
)
SELECT *
FROM (
    SELECT
        country,
        status,
        city,
        prev_prev_age_bucket
    FROM base_customers
) src
PIVOT (
    SUM(prev_prev_age_bucket)
    FOR city IN ('New York' AS new_york, 'Singapore' AS singapore)
)
