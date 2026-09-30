WITH joined AS (
    -- Attach orders attributes
    SELECT e.*, d.channel
    FROM payments e
    JOIN orders d
        ON e.order_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY status, channel) AS status_avg_amount,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY amount NULLS FIRST) AS amount_cume_dist
    FROM joined
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        amount,
        status,
        status_avg_amount,
        amount_cume_dist,
        CONCAT_WS('-', channel, method) AS channel_method_key
    FROM metrics
    WHERE order_id IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, amount_cume_dist * status_avg_amount AS amount_cume_dist_times_status_avg_amount
    FROM cleaned
    WHERE amount >= 1540
),
cleaned_payments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        status,
        status_avg_amount,
        amount_cume_dist,
        channel_method_key,
        amount_cume_dist_times_status_avg_amount,
        LOWER(status) AS status_lower,
        LOWER(status) AS status_lower_2
    FROM filtered
    WHERE order_id IS NOT NULL
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            order_id,
            status,
            amount_cume_dist_times_status_avg_amount
        FROM cleaned_payments
    ) src
    PIVOT (
        SUM(amount_cume_dist_times_status_avg_amount)
        FOR status IN ('SUCCESS', 'REFUNDED', 'FAILED')
    )
)
SELECT
    s.order_id,
    s.FAILED,
    (SELECT AVG(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS avg_order_item_quantity
FROM pivoted s
ORDER BY FAILED DESC;
