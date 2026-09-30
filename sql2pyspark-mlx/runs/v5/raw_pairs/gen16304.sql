WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('YEAR', end_date) end_date_period,
        LAST_DAY(start_date) start_date_month_end
    FROM subscriptions
    WHERE start_date >= DATE '2023-06-07'
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(monthly_fee) OVER (ORDER BY CAST(end_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) monthly_fee_count_30d,
        AVG(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY monthly_fee, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) monthly_fee_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date_month_end, id) seq_no,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC) monthly_fee_cume_dist
    FROM base
    WHERE end_date BETWEEN '2024-04-08' AND '2025-07-01'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee_cume_dist DESC NULLS LAST, id) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM latest a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date >= sub.start_date
       AND a.start_date < sub.end_date + INTERVAL 14 DAYS
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.end_date,
    s.status,
    s.seq_no,
    s.monthly_fee_cume_dist,
    s.dedup_rank,
    s.subscription_id,
    CASE WHEN status RLIKE '^(PAUSED|CANCELLED)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(ACTIVE|EXPIRED)$' THEN 'PRIORITY' ELSE 'CORE' END AS status_zone,
    (SELECT SUM(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_discount
FROM matched s
ORDER BY id NULLS LAST
