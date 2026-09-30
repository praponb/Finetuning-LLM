WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit NULLS LAST, id) AS latest_rank
    FROM customers
    QUALIFY latest_rank = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        country,
        signup_date,
        credit_limit,
        age * latest_rank age_times_latest_rank,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END is_new_york
    FROM ranked
    WHERE signup_date > DATE '2023-06-15'
      AND segment IS NOT NULL
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.subscription_start_date
    FROM valid s
    JOIN LATERAL (
            SELECT c.start_date AS subscription_start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    name,
    credit_limit,
    is_new_york,
    subscription_start_date
FROM with_last
ORDER BY is_new_york DESC;
