WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(plan) clean_plan,
        FLOOR(monthly_fee) monthly_fee_whole
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-10-03')
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM filtered a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date >= sub.start_date
       AND a.start_date <= DATE_ADD(sub.end_date, 3)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee_whole DESC NULLS LAST, id, subscription_id) AS row_num
    FROM attributed
    QUALIFY row_num = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.start_date,
        a.end_date,
        a.status,
        a.monthly_fee_whole,
        a.subscription_id,
        p.credit_limit,
        p.segment,
        p.city
    FROM deduped a
    JOIN customers p
        ON a.customer_id = p.id
    WHERE p.city != 'Bangkok'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        start_date,
        status,
        monthly_fee_whole,
        subscription_id,
        city,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY plan), 4) credit_limit_share,
        AVG(monthly_fee_whole) OVER (ORDER BY subscription_id, id, subscription_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) monthly_fee_whole_avg_3_prior,
        LAG(monthly_fee_whole, 2) OVER (PARTITION BY city ORDER BY subscription_id, id, subscription_id) prev_monthly_fee_whole
    FROM joined
),
deduped_subscriptions AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee_whole,
        subscription_id,
        monthly_fee_whole_avg_3_prior,
        prev_monthly_fee_whole
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee_whole DESC NULLS LAST, id, subscription_id) = 1
),
enriched_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(monthly_fee_whole) OVER (PARTITION BY customer_id ORDER BY prev_monthly_fee_whole, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_monthly_fee_whole,
        SUM(monthly_fee_whole) OVER (PARTITION BY customer_id ORDER BY prev_monthly_fee_whole, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_monthly_fee_whole,
        MIN(monthly_fee_whole) OVER (PARTITION BY customer_id ORDER BY prev_monthly_fee_whole, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_monthly_fee_whole,
        MAX(monthly_fee_whole) OVER (PARTITION BY customer_id ORDER BY prev_monthly_fee_whole, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_monthly_fee_whole,
        LEAD(subscription_id, 1) OVER (PARTITION BY customer_id ORDER BY start_date, id, subscription_id) next_subscription_id
    FROM deduped_subscriptions
)
-- Join a dimension and aggregate
SELECT
    p.segment,
    COUNT(*) AS num_rows,
    MIN(a.monthly_fee_whole_avg_3_prior) AS min_monthly_fee_whole_avg_3_prior,
    GROUPING_ID() AS level_id
FROM enriched_subscriptions a
JOIN customers p ON a.customer_id = p.id
GROUP BY ROLLUP (p.segment)
ORDER BY min_monthly_fee_whole_avg_3_prior;
