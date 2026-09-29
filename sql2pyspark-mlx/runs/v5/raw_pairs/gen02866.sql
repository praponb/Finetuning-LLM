WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at >= s.order_date
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM kept
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        COALESCE(discount, 0) AS discount_filled,
        DATE_ADD(order_date, 84) AS order_date_plus_84d
    FROM latest
    WHERE order_date >= DATE '2023-04-21'
      AND channel IS NOT DISTINCT FROM 'web'
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.order_date,
        a.amount,
        a.discount_filled,
        a.order_date_plus_84d,
        b.sqft,
        b.opened_date,
        b.store_name
    FROM valid a
    LEFT JOIN stores b
        ON a.store_id <=> b.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        order_date,
        amount,
        sqft,
        opened_date,
        store_name,
        MAX(discount_filled) OVER (PARTITION BY store_id ORDER BY discount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount_filled,
        SUM(discount_filled) OVER (PARTITION BY store_id ORDER BY discount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_discount_filled,
        AVG(discount_filled) OVER (PARTITION BY store_id ORDER BY discount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount_filled,
        MIN(discount_filled) OVER (PARTITION BY store_id ORDER BY discount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount_filled,
        ROUND(amount / SUM(amount) OVER (PARTITION BY store_id), 4) AS amount_share
    FROM with_dims
),
metrics_orders AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        order_date,
        sqft,
        running_max_discount_filled,
        running_sum_discount_filled,
        running_avg_discount_filled,
        running_min_discount_filled,
        amount_share,
        LEAD(running_sum_discount_filled) OVER w_hist AS next_running_sum_discount_filled,
        LAG(running_sum_discount_filled) OVER w_hist AS prev_running_sum_discount_filled,
        LEAD(running_avg_discount_filled) OVER w_hist AS next_running_avg_discount_filled,
        LAG(running_avg_discount_filled) OVER w_hist AS prev_running_avg_discount_filled,
        ROW_NUMBER() OVER w_hist AS event_seq,
        PERCENT_RANK() OVER (ORDER BY sqft) AS sqft_percent_rank
    FROM metrics
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY opened_date, id)
)
SELECT
    id,
    sqft,
    running_max_discount_filled,
    running_min_discount_filled,
    amount_share,
    next_running_sum_discount_filled,
    next_running_avg_discount_filled,
    prev_running_avg_discount_filled,
    event_seq
FROM metrics_orders;
