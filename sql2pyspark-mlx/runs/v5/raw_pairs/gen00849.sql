WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE segment IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(credit_limit, 0) AS credit_limit_filled,
        COALESCE(segment, 'DIRECT') AS segment_clean
    FROM base
    WHERE segment IS DISTINCT FROM 'Corporate'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        is_active,
        email_user,
        credit_limit_filled,
        LAG(credit_limit_filled) OVER (PARTITION BY city, segment ORDER BY age, id) AS prev_credit_limit_filled,
        LEAD(credit_limit_filled) OVER (PARTITION BY city, segment ORDER BY age, id) AS next_credit_limit_filled,
        LAG(credit_limit) OVER (PARTITION BY city, segment ORDER BY age, id) AS prev_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY age, id) AS row_seq,
        NTILE(3) OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS credit_limit_bucket
    FROM filtered
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email_user,
        credit_limit_filled,
        prev_credit_limit_filled,
        next_credit_limit_filled,
        LAG(credit_limit_bucket, 1) OVER (ORDER BY next_credit_limit_filled, id) AS prev_credit_limit_bucket,
        LAST_VALUE(prev_credit_limit) IGNORE NULLS OVER (ORDER BY next_credit_limit_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_credit_limit,
        NTILE(3) OVER (ORDER BY next_credit_limit_filled DESC, id) AS next_credit_limit_filled_bucket,
        NTILE(10) OVER (ORDER BY row_seq, id) AS row_seq_bucket,
        LAST_VALUE(credit_limit_filled) IGNORE NULLS OVER (ORDER BY prev_credit_limit_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit_filled
    FROM with_windows
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY prev_credit_limit_bucket, id) AS prev_credit_limit_bucket_bucket,
        SUM(next_credit_limit_filled_bucket) OVER (ORDER BY next_credit_limit_filled_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_credit_limit_filled_bucket,
        ROW_NUMBER() OVER (ORDER BY last_known_credit_limit_filled DESC, id) AS event_seq,
        PERCENT_RANK() OVER (ORDER BY last_known_prev_credit_limit DESC NULLS FIRST) AS last_known_prev_credit_limit_percent_rank
    FROM enriched
),
with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.credit_limit_filled,
        s.next_credit_limit_filled,
        s.last_known_prev_credit_limit,
        s.row_seq_bucket,
        s.last_known_credit_limit_filled,
        s.prev_credit_limit_bucket_bucket,
        s.event_seq,
        s.last_known_prev_credit_limit_percent_rank,
        top1.account_opened_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.opened_date AS account_opened_date
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.credit_limit_filled,
        s.next_credit_limit_filled,
        s.row_seq_bucket,
        s.last_known_credit_limit_filled,
        top1.order_date,
        top1.order_shipping_fee
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.order_date, c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    credit_limit_filled,
    next_credit_limit_filled,
    last_known_credit_limit_filled,
    order_date,
    order_shipping_fee
FROM latest_match
ORDER BY last_known_credit_limit_filled DESC NULLS FIRST;
