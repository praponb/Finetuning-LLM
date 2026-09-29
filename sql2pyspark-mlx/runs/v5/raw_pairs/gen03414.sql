WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        MIN(amount) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_amount,
        SUM(amount) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_amount,
        MAX(amount) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_amount,
        AVG(amount) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_amount
    FROM orders
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        channel,
        discount,
        shipping_fee,
        running_min_amount,
        running_sum_amount,
        running_max_amount,
        running_avg_amount,
        ROUND(running_max_amount, 2) running_max_amount_rounded
    FROM enriched
    WHERE order_date >= DATE '2023-04-13'
      AND channel <> 'store'
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.running_max_amount,
        e.running_avg_amount,
        e.running_max_amount_rounded,
        b.store_name,
        b.opened_date,
        b.sqft
    FROM filtered e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        running_avg_amount,
        running_max_amount_rounded,
        store_name,
        LEAD(running_max_amount_rounded) OVER (PARTITION BY store_id ORDER BY opened_date, id) next_running_max_amount_rounded,
        LEAD(sqft) OVER (PARTITION BY store_id ORDER BY opened_date, id) next_sqft,
        LAG(sqft) OVER (PARTITION BY store_id ORDER BY opened_date, id) prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY opened_date, id) event_seq,
        FIRST_VALUE(running_avg_amount) IGNORE NULLS OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_running_avg_amount
    FROM joined
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        running_avg_amount,
        running_max_amount_rounded,
        store_name,
        next_running_max_amount_rounded,
        prev_sqft,
        event_seq,
        first_known_running_avg_amount,
        LEAD(first_known_running_avg_amount, 1) OVER (PARTITION BY store_id ORDER BY next_running_max_amount_rounded, id) next_first_known_running_avg_amount,
        LAG(next_sqft, 1) OVER (PARTITION BY store_id ORDER BY event_seq, id) prev_next_sqft,
        SUM(running_avg_amount) OVER (PARTITION BY store_id ORDER BY running_avg_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_running_avg_amount,
        ROUND(first_known_running_avg_amount / SUM(first_known_running_avg_amount) OVER (PARTITION BY store_id), 4) first_known_running_avg_amount_share,
        NTILE(10) OVER (ORDER BY prev_sqft DESC, id) prev_sqft_bucket
    FROM metrics
    WHERE next_sqft BETWEEN 5438 AND 15612
)
-- Ranking and gap features
SELECT
    id,
    store_id,
    running_avg_amount,
    running_max_amount_rounded,
    store_name,
    next_running_max_amount_rounded,
    prev_sqft,
    first_known_running_avg_amount,
    prev_next_sqft,
    cumulative_running_avg_amount,
    first_known_running_avg_amount_share,
    AVG(cumulative_running_avg_amount) OVER (PARTITION BY store_id ORDER BY running_avg_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cumulative_running_avg_amount_avg_3,
    SUM(first_known_running_avg_amount) OVER (PARTITION BY store_id ORDER BY prev_sqft_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_first_known_running_avg_amount,
    LAG(first_known_running_avg_amount, 2) OVER (PARTITION BY store_id ORDER BY next_first_known_running_avg_amount, id) prev_first_known_running_avg_amount
FROM with_windows;
