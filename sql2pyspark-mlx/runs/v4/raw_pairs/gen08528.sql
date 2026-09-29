WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(order_date, 12) AS order_date_plus_months,
        shipping_fee * amount AS amount_with_shipping
    FROM orders
    WHERE order_date >= TO_DATE('2024-04-18')
      AND customer_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY channel ORDER BY amount_with_shipping, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2,
        SUM(amount_with_shipping) OVER w_grp AS cumulative_amount_with_shipping
    FROM valid
    WINDOW w_grp AS (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_orders AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(amount_with_shipping) OVER (PARTITION BY customer_id) AS customer_id_max_amount_with_shipping,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY order_date DESC, id) AS seq_no
    FROM scored
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored_orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.order_id IN (1, 21, 31)
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.quantity != 3
        )
),
kept_orders AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.delivered_date <= s.order_date_plus_months
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM kept_orders
    EXCEPT
    SELECT store_id
    FROM orders
    WHERE amount >= 1150
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_orders s
    JOIN kept_keys k ON s.store_id = k.store_id
)
SELECT
    s.customer_id,
    s.store_id,
    s.order_date,
    s.status,
    s.channel,
    s.amount,
    s.shipping_fee,
    s.order_date_plus_months,
    s.amount_with_shipping,
    (SELECT AVG(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS avg_inventory_quantity
FROM reconciled s
ORDER BY amount NULLS FIRST;
