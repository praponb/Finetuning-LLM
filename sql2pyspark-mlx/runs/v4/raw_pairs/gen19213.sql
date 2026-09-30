WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(shipping_fee AS INT) AS shipping_fee_int,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM orders
    WHERE order_date > TO_DATE('2023-10-13')
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.shipment_cost
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.cost AS shipment_cost
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status
    FROM latest_match
    EXCEPT
    SELECT status
    FROM orders
    WHERE status IN ('PENDING', 'SHIPPED', 'COMPLETED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    INNER JOIN remaining_keys k ON s.status = k.status
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY store_id, customer_id ORDER BY amount DESC) AS amount_rank,
        CORR(shipping_fee, amount) OVER w_recent AS corr_shipping_fee_amount
    FROM kept_rows
    WINDOW w_recent AS (PARTITION BY channel, status)
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.channel,
    s.amount,
    s.discount,
    s.shipping_fee_int,
    s.channel_clean,
    (SELECT MAX(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS max_account_balance
FROM metrics s
ORDER BY customer_id;
