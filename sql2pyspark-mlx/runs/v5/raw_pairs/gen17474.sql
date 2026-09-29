WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        is_active,
        MIN(age) OVER w_hist running_min_age,
        SUM(age) OVER w_hist running_sum_age,
        COUNT(age) OVER w_hist running_count_age,
        AVG(age) OVER w_hist running_avg_age,
        ROW_NUMBER() OVER w_running event_seq
    FROM customers
    WINDOW w_hist AS (PARTITION BY city, country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY city ORDER BY age DESC, id)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        is_active,
        running_sum_age,
        running_avg_age,
        event_seq,
        ROUND(event_seq, 0) event_seq_rounded,
        COALESCE(name, 'Unknown') name_filled
    FROM scored
    WHERE running_count_age >= 5
),
base_customers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        is_active,
        running_avg_age,
        event_seq,
        event_seq_rounded,
        name_filled,
        running_sum_age - event_seq_rounded running_sum_age_minus_event_seq_rounded,
        INITCAP(TRIM(name)) name_title
    FROM base
    WHERE running_avg_age > 59
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        is_active,
        running_avg_age,
        event_seq,
        event_seq_rounded,
        name_filled,
        name_title,
        running_sum_age_minus_event_seq_rounded + running_avg_age running_sum_age_minus_event_seq_rounded_plus_running_avg_age
    FROM base_customers
    WHERE running_sum_age_minus_event_seq_rounded >= 24
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY running_avg_age DESC, id) running_avg_age_bucket,
        ROW_NUMBER() OVER (ORDER BY event_seq_rounded, id) event_seq_2
    FROM valid
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.event_seq_rounded,
        s.name_filled,
        s.name_title,
        s.event_seq_2,
        latest.order_date,
        latest.shipping_fee
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.order_date, c.shipping_fee
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.order_date
        )
)
-- Running and moving aggregates
SELECT
    id,
    name,
    name_filled,
    name_title,
    shipping_fee,
    ROW_NUMBER() OVER (ORDER BY event_seq_rounded, id) event_seq,
    LAST_VALUE(event_seq_rounded) IGNORE NULLS OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_event_seq_rounded,
    ROW_NUMBER() OVER (ORDER BY event_seq_rounded, id) seq_no,
    LEAD(event_seq_rounded, 1) OVER (ORDER BY shipping_fee, id) next_event_seq_rounded
FROM not_flagged
