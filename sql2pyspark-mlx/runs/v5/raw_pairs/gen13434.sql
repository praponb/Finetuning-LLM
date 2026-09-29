WITH enriched AS (
    SELECT
        src.id,
        src.customer_id,
        src.start_date,
        src.end_date,
        src.monthly_fee,
        src.status,
        d.signup_date,
        d.city,
        d.credit_limit
    FROM subscriptions src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY end_date NULLS LAST, id) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
with_windows AS (
    SELECT
        id,
        end_date,
        monthly_fee,
        status,
        city,
        credit_limit,
        row_num,
        MAX(credit_limit) OVER (PARTITION BY status ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY status ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_credit_limit,
        COUNT(credit_limit) OVER (PARTITION BY status ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_credit_limit,
        MIN(monthly_fee) OVER (PARTITION BY status) AS status_min_monthly_fee
    FROM ranked
    WHERE YEAR(start_date) = 2025
),
metrics AS (
    SELECT
        *,
        AVG(status_min_monthly_fee) OVER (PARTITION BY city ORDER BY last3_count_credit_limit, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_min_monthly_fee_avg_2_prior,
        AVG(last3_max_credit_limit) OVER w_ordered AS last3_max_credit_limit_avg_3
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
ranked_subscriptions AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY last3_max_credit_limit_avg_3 NULLS LAST, id) AS dedup_rank
        FROM metrics
    ) t
    WHERE dedup_rank = 1
)
SELECT
    id,
    status,
    city,
    credit_limit,
    last3_max_credit_limit,
    status_min_monthly_fee,
    status_min_monthly_fee_avg_2_prior,
    dedup_rank
FROM ranked_subscriptions
ORDER BY credit_limit
