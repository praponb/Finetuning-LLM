WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        CONCAT_WS('/', channel, status) AS channel_status_key,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM orders
    WHERE store_id IS NOT NULL
      AND channel = 'mobile'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM filtered
    EXCEPT DISTINCT
    SELECT customer_id
    FROM orders
    WHERE shipping_fee IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.cost <= 65
        )
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(order_date) AS order_date_month_end
    FROM kept
    WHERE order_date > DATE '2024-03-19'
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_running AS shipping_fee_cume_dist,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        CUME_DIST() OVER w_running AS shipping_fee_cume_dist_2,
        MAX(amount) OVER w_part AS status_max_amount,
        SUM(amount) OVER (PARTITION BY status, customer_id) AS status_sum_amount
    FROM base
    WINDOW w_running AS (ORDER BY shipping_fee DESC), w_part AS (PARTITION BY status)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        channel_status_key,
        channel_clean,
        order_date_month_end,
        shipping_fee_cume_dist,
        amount_share,
        shipping_fee_cume_dist_2,
        status_max_amount,
        status_sum_amount,
        MAX(shipping_fee) OVER (PARTITION BY status, customer_id) AS status_max_shipping_fee,
        MAX(shipping_fee_cume_dist_2) OVER (PARTITION BY customer_id) AS customer_id_max_shipping_fee_cume_dist_2,
        MAX(amount_share) OVER (PARTITION BY customer_id, status) AS customer_id_max_amount_share,
        COUNT(amount_share) OVER (PARTITION BY customer_id) AS customer_id_count_amount_share
    FROM metrics
),
kept_orders AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at >= s.order_date
        )
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            customer_id,
            status,
            shipping_fee_cume_dist
        FROM kept_orders
    ) src
    PIVOT (
        MAX(shipping_fee_cume_dist) AS amount,
        COUNT(1) AS n
        FOR status IN ('CANCELLED' AS cancelled, 'PENDING' AS pending, 'COMPLETED' AS completed)
    )
)
SELECT
    customer_id,
    cancelled_amount,
    pending_amount,
    completed_amount,
    completed_n
FROM wide
