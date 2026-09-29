WITH metrics AS (
    SELECT
        *,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY customer_id, status) AS customer_id_stddev_samp_monthly_fee,
        FIRST_VALUE(monthly_fee, TRUE) OVER (ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee
    FROM subscriptions
),
with_window AS (
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM metrics e
    INNER JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.end_date BETWEEN sub.start_date AND sub.end_date
),
with_windows AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        customer_id_stddev_samp_monthly_fee,
        first_known_monthly_fee,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY subscription_monthly_fee ASC NULLS LAST) AS subscription_monthly_fee_percent_rank,
        LAG(first_known_monthly_fee, 1) OVER (PARTITION BY customer_id ORDER BY subscription_id ASC, id ASC, subscription_id ASC) AS prev_first_known_monthly_fee,
        AVG(subscription_id) OVER (PARTITION BY plan ORDER BY monthly_fee ASC, id ASC, subscription_id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS subscription_id_avg_5_prior,
        AVG(subscription_monthly_fee) OVER (PARTITION BY customer_id, subscription_plan) AS customer_id_avg_subscription_monthly_fee
    FROM with_window
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    customer_id_stddev_samp_monthly_fee,
    first_known_monthly_fee,
    subscription_id,
    subscription_plan,
    subscription_monthly_fee,
    subscription_monthly_fee_percent_rank,
    prev_first_known_monthly_fee,
    subscription_id_avg_5_prior,
    customer_id_avg_subscription_monthly_fee,
    FIRST_VALUE(first_known_monthly_fee, TRUE) OVER (PARTITION BY subscription_plan, status ORDER BY subscription_monthly_fee_percent_rank ASC, id ASC, subscription_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_monthly_fee,
    MIN(subscription_monthly_fee_percent_rank) OVER (PARTITION BY status) AS status_min_subscription_monthly_fee_percent_rank,
    LEAD(first_known_monthly_fee, 1) OVER (ORDER BY subscription_id_avg_5_prior ASC, id ASC, subscription_id ASC) AS next_first_known_monthly_fee,
    LEAD(subscription_monthly_fee, 1) OVER w_hist AS next_subscription_monthly_fee
FROM with_windows
WINDOW w_hist AS (ORDER BY end_date ASC, id ASC, subscription_id ASC);
