WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS rank_in_group
    FROM customers
    QUALIFY rank_in_group = 1
),
with_windows AS (
    SELECT
        id,
        email,
        city,
        segment,
        credit_limit,
        age,
        LAG(credit_limit) OVER (PARTITION BY city ORDER BY age, id) AS prev_credit_limit,
        LEAD(credit_limit) OVER (PARTITION BY city ORDER BY age, id) AS next_credit_limit,
        LAG(age) OVER (PARTITION BY city ORDER BY age, id) AS prev_age,
        LEAD(age) OVER (PARTITION BY city ORDER BY age, id) AS next_age,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY age, id) AS row_seq,
        LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY country ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
    FROM latest
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY next_age NULLS FIRST, id) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
latest_match AS (
    SELECT s.*, latest.end_date
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match_customers AS (
    SELECT s.*, x.subscription_end_date
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.end_date AS subscription_end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    city,
    segment,
    credit_limit,
    age,
    prev_credit_limit,
    prev_age,
    next_age,
    dedup_rank,
    CASE WHEN city RLIKE '^(Berlin|Singapore)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS city_zone
FROM latest_match_customers
ORDER BY id
