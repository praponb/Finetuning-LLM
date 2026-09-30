WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY city, country ORDER BY age, id) age_bucket,
        NTILE(5) OVER (PARTITION BY city ORDER BY age DESC, id) age_bucket_2
    FROM customers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(age_bucket_2, 1) age_bucket_2_rounded,
        ROUND(credit_limit, 0) credit_limit_rounded
    FROM scored
    WHERE signup_date >= DATE '2023-12-19'
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_status
    FROM valid s
    JOIN LATERAL (
            SELECT c.status AS order_status
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        age_bucket_2,
        age_bucket_2_rounded,
        credit_limit_rounded,
        order_status,
        IF(is_active, 'Yes', 'No') is_active_label,
        COALESCE(order_status, 'N/A') resolved_order_status
    FROM with_latest
    WHERE city IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY order_status ORDER BY credit_limit DESC, id) credit_limit_bucket,
        NTILE(4) OVER (PARTITION BY order_status ORDER BY credit_limit_rounded, id) credit_limit_rounded_bucket
    FROM filtered
),
with_windows_customers AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(age_bucket_2_rounded) OVER (PARTITION BY order_status ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) age_bucket_2_rounded_avg_5_prior,
        NTILE(5) OVER (PARTITION BY country ORDER BY age_bucket_2_rounded DESC, id) age_bucket_2_rounded_bucket,
        PERCENT_RANK() OVER (ORDER BY credit_limit_bucket DESC NULLS LAST) credit_limit_bucket_percent_rank
    FROM with_windows
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.start_date < s.signup_date
        )
)
-- Running and moving aggregates
SELECT
    *,
    ROW_NUMBER() OVER w_grp event_seq,
    MAX(credit_limit_bucket_percent_rank) OVER (PARTITION BY country) country_max_credit_limit_bucket_percent_rank
FROM not_flagged
WINDOW w_grp AS (PARTITION BY country ORDER BY signup_date DESC, id)
