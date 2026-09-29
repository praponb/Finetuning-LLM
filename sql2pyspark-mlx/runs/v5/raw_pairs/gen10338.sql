WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', plan, status) AS plan_status_key
    FROM subscriptions
    WHERE start_date > DATE '2023-06-12'
      AND end_date BETWEEN '2023-10-01' AND '2024-09-01'
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, c.city
    FROM valid e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        plan_status_key,
        LEAD(monthly_fee, 2) OVER (ORDER BY end_date, id) AS next_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY city), 4) AS monthly_fee_share
    FROM with_dims
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(next_monthly_fee) IGNORE NULLS OVER (PARTITION BY plan ORDER BY monthly_fee_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_monthly_fee,
        RANK() OVER (ORDER BY monthly_fee_share NULLS LAST) AS monthly_fee_share_rank
    FROM metrics
)
SELECT
    s.id,
    s.customer_id,
    s.end_date,
    s.status,
    s.plan_status_key,
    s.monthly_fee_share,
    s.monthly_fee_share_rank,
    CASE WHEN status RLIKE '^(ACTIVE|CANCELLED)$' THEN 'EXTENDED' WHEN status RLIKE '^(PAUSED|EXPIRED)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS status_zone,
    (SELECT MIN(x.amount) FROM orders x WHERE x.customer_id = s.customer_id) AS min_order_amount
FROM scored s
ORDER BY end_date DESC NULLS LAST
