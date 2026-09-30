WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY end_date, id) AS rn
    FROM subscriptions
    QUALIFY rn = 1
),
base AS (
    SELECT
        *,
        UPPER(status) status_upper,
        SUBSTRING(plan, 1, 2) plan_prefix
    FROM ranked
    WHERE MONTH(end_date) IN (2, 3, 10)
),
attributed AS (
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM base s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.end_date >= sub.start_date
       AND s.end_date <= DATE_ADD(sub.end_date, 7)
),
scored AS (
    SELECT
        *,
        SUM(subscription_monthly_fee) OVER (PARTITION BY status ORDER BY subscription_monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_subscription_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date, id, subscription_id) event_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id, subscription_id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date, id, subscription_id) row_seq_2
    FROM attributed
),
filtered AS (
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        rn,
        status_upper,
        plan_prefix,
        subscription_id,
        subscription_plan,
        cumulative_subscription_monthly_fee,
        event_seq,
        row_seq,
        row_seq_2,
        CASE WHEN event_seq >= 7 THEN 'High' WHEN event_seq >= 4 THEN 'Medium' ELSE 'Low' END event_seq_tier
    FROM scored
    WHERE start_date >= DATE '2023-12-09'
),
filtered_subscriptions AS (
    SELECT
        id,
        start_date,
        monthly_fee,
        status,
        rn,
        status_upper,
        subscription_id,
        subscription_plan,
        cumulative_subscription_monthly_fee,
        row_seq,
        row_seq_2,
        event_seq_tier,
        LEAST(row_seq_2, event_seq) least_row_seq_2_event_seq,
        ROUND(cumulative_subscription_monthly_fee / row_seq, 2) cumulative_subscription_monthly_fee_per_row_seq
    FROM filtered
    WHERE rn >= 1
),
filtered_subscriptions_2 AS (
    SELECT
        id,
        start_date,
        monthly_fee,
        status_upper,
        subscription_id,
        subscription_plan,
        cumulative_subscription_monthly_fee,
        row_seq,
        row_seq_2,
        event_seq_tier,
        least_row_seq_2_event_seq,
        cumulative_subscription_monthly_fee_per_row_seq,
        DATE_TRUNC('YEAR', start_date) start_date_period
    FROM filtered_subscriptions
    WHERE start_date >= TO_DATE('2023-10-17')
)
SELECT
    id,
    start_date,
    monthly_fee,
    status_upper,
    subscription_id,
    subscription_plan,
    cumulative_subscription_monthly_fee,
    row_seq_2,
    event_seq_tier
FROM filtered_subscriptions_2
ORDER BY cumulative_subscription_monthly_fee DESC, row_seq_2 DESC
