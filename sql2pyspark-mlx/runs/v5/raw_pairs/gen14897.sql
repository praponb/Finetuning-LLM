WITH enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.signup_date,
        d.email,
        d.credit_limit
    FROM subscriptions s
    INNER JOIN customers d
        ON s.customer_id = d.id
    WHERE d.country = 'TH'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY credit_limit DESC, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        signup_date,
        email,
        credit_limit,
        MIN(credit_limit) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_credit_limit,
        MAX(credit_limit) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_credit_limit,
        COUNT(credit_limit) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_credit_limit,
        AVG(credit_limit) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_credit_limit,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit
    FROM ranked
),
enriched_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        signup_date,
        credit_limit,
        running_max_credit_limit,
        running_avg_credit_limit,
        last_known_credit_limit,
        COUNT(last_known_credit_limit) OVER w_recent AS last7_count_last_known_credit_limit,
        MIN(last_known_credit_limit) OVER w_recent AS last7_min_last_known_credit_limit,
        AVG(last_known_credit_limit) OVER w_recent AS last7_avg_last_known_credit_limit
    FROM scored
    WINDOW w_recent AS (PARTITION BY plan ORDER BY running_min_credit_limit, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    signup_date,
    running_max_credit_limit,
    running_avg_credit_limit,
    last_known_credit_limit,
    last7_min_last_known_credit_limit,
    last7_avg_last_known_credit_limit
FROM enriched_subscriptions
ORDER BY last7_min_last_known_credit_limit NULLS LAST, last7_avg_last_known_credit_limit DESC;
