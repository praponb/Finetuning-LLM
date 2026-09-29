WITH latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.account_balance
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.balance AS account_balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        country,
        city,
        is_active,
        credit_limit,
        age,
        DENSE_RANK() OVER (ORDER BY age NULLS LAST) AS age_dense_rank,
        DENSE_RANK() OVER (ORDER BY account_balance) AS account_balance_dense_rank,
        AVG(account_balance) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS account_balance_avg_5
    FROM latest_match
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.order_status,
        latest.order_channel
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status, c.channel AS order_channel
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(age_dense_rank, 2, 0) OVER (ORDER BY credit_limit, id) AS prev_age_dense_rank,
        AVG(age_dense_rank) OVER (PARTITION BY country ORDER BY age_dense_rank, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_dense_rank_avg_5,
        AVG(age) OVER (PARTITION BY order_status, country ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS age_avg_6_prior,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY age_dense_rank) AS age_dense_rank_percent_rank
    FROM with_last
)
SELECT
    id,
    email,
    city,
    is_active,
    credit_limit,
    age,
    age_dense_rank,
    account_balance_dense_rank,
    order_status,
    CASE WHEN city RLIKE '^(London|Singapore)$' THEN 'CORE' ELSE 'PRIORITY' END AS city_zone
FROM with_windows
ORDER BY order_status DESC NULLS LAST, id;
