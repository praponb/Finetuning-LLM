WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_grp AS monthly_fee_cume_dist,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
    FROM subscriptions
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS LAST)
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM metrics src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.end_date BETWEEN sub.start_date AND sub.end_date
),
metrics_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_cume_dist,
        running_monthly_fee,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        AVG(subscription_monthly_fee) OVER (PARTITION BY status ORDER BY subscription_monthly_fee, id, subscription_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS subscription_monthly_fee_avg_3,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        SUM(subscription_id) OVER w_part AS running_subscription_id
    FROM with_window
    WINDOW w_ordered AS (PARTITION BY status ORDER BY subscription_id DESC, id, subscription_id), w_part AS (PARTITION BY customer_id ORDER BY start_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_subscriptions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan, subscription_plan ORDER BY subscription_monthly_fee_avg_3) AS subscription_monthly_fee_avg_3_percent_rank,
        NTILE(4) OVER (ORDER BY running_monthly_fee, id, subscription_id) AS running_monthly_fee_bucket
    FROM metrics_subscriptions
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_cume_dist,
        running_monthly_fee,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        subscription_monthly_fee_avg_3,
        row_seq,
        running_subscription_id,
        subscription_monthly_fee_avg_3_percent_rank,
        running_monthly_fee_bucket,
        NTILE(3) OVER (PARTITION BY plan ORDER BY monthly_fee, id, subscription_id) AS monthly_fee_bucket,
        RANK() OVER (PARTITION BY status ORDER BY running_subscription_id) AS running_subscription_id_rank
    FROM metrics_subscriptions_2
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY running_monthly_fee DESC NULLS LAST) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        subscription_id,
        customer_id,
        subscription_monthly_fee,
        monthly_fee,
        monthly_fee_cume_dist
    FROM ranked
) src
UNPIVOT (
    value FOR kpi IN (subscription_monthly_fee AS SUBSCRIPTION_MONTHLY_FEE, monthly_fee AS MONTHLY_FEE_VAL, monthly_fee_cume_dist)
);
