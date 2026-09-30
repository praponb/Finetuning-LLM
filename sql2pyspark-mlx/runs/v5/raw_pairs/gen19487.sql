WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(email, 1, 3) AS email_prefix,
        SUBSTRING(country, 1, 4) AS country_prefix,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM customers
    WHERE city IS NOT NULL
      AND LOWER(name) LIKE '%ob %'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(country) AS country_lower,
        credit_limit * age AS credit_limit_times_age
    FROM valid
    WHERE country != 'JP'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        email_prefix,
        country_prefix,
        city_filled,
        country_lower,
        credit_limit_times_age,
        SUM(credit_limit) OVER w_ordered AS running_credit_limit,
        LEAD(credit_limit, 1) OVER (PARTITION BY segment ORDER BY credit_limit_times_age, id) AS next_credit_limit,
        AVG(age) OVER (PARTITION BY city ORDER BY credit_limit_times_age, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_avg_5
    FROM cleaned
    WINDOW w_ordered AS (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.email,
        s.country,
        s.city,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        s.email_prefix,
        s.country_prefix,
        s.credit_limit_times_age,
        s.running_credit_limit,
        top1.order_amount
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.amount AS order_amount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_customers AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.email_prefix,
        s.country_prefix,
        s.credit_limit_times_age,
        s.running_credit_limit,
        s.order_amount,
        top1.subscription_end_date
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.end_date AS subscription_end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.subscription_end_date
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        city,
        order_amount,
        subscription_end_date,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY running_credit_limit DESC) AS running_credit_limit_percent_rank,
        NTILE(4) OVER (PARTITION BY city ORDER BY credit_limit_times_age, id) AS credit_limit_times_age_bucket,
        LEAD(credit_limit_times_age, 1) OVER (PARTITION BY country ORDER BY order_amount, id) AS next_credit_limit_times_age,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY subscription_end_date, id) AS seq_no,
        AVG(order_amount) OVER (ORDER BY credit_limit_times_age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS order_amount_avg_6
    FROM not_flagged
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        country,
        running_credit_limit_percent_rank,
        next_credit_limit_times_age,
        order_amount
    FROM scored
) src
UNPIVOT (
    value FOR measure IN (running_credit_limit_percent_rank, next_credit_limit_times_age, order_amount)
);
