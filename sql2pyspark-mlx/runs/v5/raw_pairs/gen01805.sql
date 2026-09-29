WITH with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.country,
        s.city,
        s.segment,
        s.is_active,
        s.credit_limit,
        top1.account_status,
        top1.account_balance
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.status AS account_status, c.balance AS account_balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY account_balance DESC, id) AS latest_rank
    FROM with_last
    QUALIFY latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        segment,
        credit_limit,
        account_status,
        account_balance,
        latest_rank,
        SUM(account_balance) OVER (PARTITION BY country ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_account_balance,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC) AS credit_limit_percent_rank
    FROM latest
),
with_last_customers AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.order_status,
        x.order_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status, c.order_date
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY order_date DESC NULLS LAST, id) AS dedup_rank
        FROM with_last_customers
    ) ranked
    WHERE dedup_rank = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_status, country ORDER BY credit_limit DESC NULLS LAST) AS row_num
        FROM ranked
    ) x
    WHERE row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    MAX(credit_limit) AS max_credit_limit,
    SIZE(COLLECT_SET(order_status)) AS num_order_status,
    ROUND(AVG(credit_limit_percent_rank), 2) AS avg_credit_limit_percent_rank,
    SUM(credit_limit) AS total_credit_limit,
    MAX(running_account_balance) FILTER (WHERE country = 'UK') AS uk_max_running_account_balance,
    SIZE(COLLECT_SET(account_status)) AS num_account_status
FROM deduped
GROUP BY DATE_TRUNC('MONTH', order_date)
ORDER BY max_credit_limit;
