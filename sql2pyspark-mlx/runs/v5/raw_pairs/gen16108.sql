WITH with_windows AS (
    SELECT
        id,
        email,
        country,
        signup_date,
        age,
        LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city,
        MIN(credit_limit) OVER (PARTITION BY city) AS city_min_credit_limit
    FROM customers
    WHERE segment = 'SMB'
),
with_last AS (
    SELECT s.*, top1.account_account_type
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.account_type AS account_account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY account_account_type, country ORDER BY city_min_credit_limit, id) AS city_min_credit_limit_bucket,
        LEAD(city_min_credit_limit, 1) OVER (PARTITION BY account_account_type ORDER BY city_min_credit_limit, id) AS next_city_min_credit_limit,
        AVG(city_min_credit_limit) OVER (ORDER BY city_min_credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS city_min_credit_limit_avg_2
    FROM with_last
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY country, account_account_type ORDER BY city_min_credit_limit DESC NULLS LAST) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
with_latest AS (
    SELECT
        s.id,
        s.email,
        s.country,
        s.signup_date,
        s.age,
        s.city_min_credit_limit,
        s.account_account_type,
        s.city_min_credit_limit_bucket,
        s.next_city_min_credit_limit,
        top1.subscription_plan,
        top1.status
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.plan AS subscription_plan, c.status
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched_customers AS (
    SELECT
        *,
        SUM(city_min_credit_limit_bucket) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_city_min_credit_limit_bucket,
        SUM(city_min_credit_limit) OVER w_recent AS cumulative_city_min_credit_limit,
        ROUND(next_city_min_credit_limit / SUM(next_city_min_credit_limit) OVER (PARTITION BY subscription_plan, status), 4) AS next_city_min_credit_limit_share
    FROM with_latest
    WINDOW w_recent AS (PARTITION BY subscription_plan ORDER BY city_min_credit_limit_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    email,
    country,
    signup_date,
    age,
    city_min_credit_limit,
    account_account_type,
    city_min_credit_limit_bucket,
    next_city_min_credit_limit,
    CASE WHEN account_account_type RLIKE '^(SAVINGS|LOAN)$' THEN 'GROUP_A' ELSE 'CORE' END AS account_account_type_zone
FROM enriched_customers;
