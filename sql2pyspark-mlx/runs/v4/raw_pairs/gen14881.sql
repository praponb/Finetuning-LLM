WITH with_windows AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY age, id) AS seq_no,
        NTILE(3) OVER (PARTITION BY country ORDER BY credit_limit, id) AS credit_limit_bucket,
        CORR(age, credit_limit) OVER (PARTITION BY country) AS corr_age_credit_limit,
        NTILE(5) OVER (PARTITION BY segment ORDER BY age, id) AS age_bucket,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS row_seq
    FROM customers
),
with_windows_customers AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        seq_no,
        credit_limit_bucket,
        corr_age_credit_limit,
        age_bucket,
        row_seq,
        AVG(credit_limit_bucket) OVER (PARTITION BY country ORDER BY age_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS credit_limit_bucket_avg_6_prior,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) AS credit_limit_share
    FROM with_windows
),
latest_match AS (
    SELECT
        s.*,
        x.order_channel,
        x.discount
    FROM with_windows_customers s
    LEFT JOIN LATERAL (
            SELECT c.channel AS order_channel, c.discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        seq_no,
        credit_limit_bucket,
        corr_age_credit_limit,
        age_bucket,
        row_seq,
        credit_limit_bucket_avg_6_prior,
        credit_limit_share,
        order_channel,
        discount,
        AVG(seq_no) OVER (PARTITION BY order_channel ORDER BY corr_age_credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS seq_no_avg_2,
        RANK() OVER (ORDER BY age_bucket NULLS FIRST) AS age_bucket_rank,
        FIRST_VALUE(order_channel) IGNORE NULLS OVER (PARTITION BY city, segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_order_channel
    FROM latest_match
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY segment ORDER BY credit_limit_share DESC NULLS FIRST) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
remaining_keys AS (
    SELECT country, city
    FROM latest
    INTERSECT ALL
    SELECT country, city
    FROM customers
    WHERE email LIKE '%@gmail.com'
),
kept_rows AS (
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.city = k.city
)
SELECT *
FROM (
    SELECT
        city,
        credit_limit_share,
        corr_age_credit_limit,
        discount
    FROM kept_rows
) src
UNPIVOT (
    value FOR metric IN (credit_limit_share AS CREDIT_LIMIT_SHARE_VAL, corr_age_credit_limit AS CORR_AGE_CREDIT_LIMIT, discount)
);
