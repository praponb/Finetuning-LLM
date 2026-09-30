WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY channel ORDER BY shipping_fee, id) AS shipping_fee_bucket,
        FIRST_VALUE(discount) IGNORE NULLS OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount
    FROM orders
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_windows src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.order_date >= sub.start_date
       AND src.order_date < sub.end_date + INTERVAL 7 DAYS
),
with_window_orders AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_window a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.order_date >= sub.start_date
       AND a.order_date <= DATE_ADD(sub.end_date, 3)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, store_id
    FROM with_window_orders
    EXCEPT
    SELECT id, store_id
    FROM orders
    WHERE channel = 'partner'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_window_orders s
    INNER JOIN matched_keys k ON s.id = k.id AND s.store_id = k.store_id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.age,
        d.name
    FROM verified s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        shipping_fee_bucket,
        first_known_discount,
        subscription_id,
        plan,
        monthly_fee,
        subscription_id_2,
        subscription_plan,
        subscription_monthly_fee,
        age,
        name,
        CORR(discount, amount) OVER (PARTITION BY plan) AS corr_discount_amount,
        CORR(discount, amount) OVER (PARTITION BY store_id, channel) AS corr_discount_amount_2
    FROM enriched
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            store_id,
            status,
            subscription_id
        FROM scored
    ) src
    PIVOT (
        AVG(subscription_id) AS amount
        FOR status IN ('PENDING' AS pending, 'CANCELLED' AS cancelled)
    )
)
SELECT
    s.store_id,
    s.pending,
    s.cancelled,
    (SELECT SUM(i2.quantity) FROM inventory i2 WHERE i2.store_id = s.store_id) AS sum_inventory_quantity
FROM pivoted s
ORDER BY store_id DESC NULLS LAST, cancelled
