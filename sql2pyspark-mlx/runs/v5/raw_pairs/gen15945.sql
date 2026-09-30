WITH base AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(discount, shipping_fee) AS greatest_discount_shipping_fee
    FROM orders
    WHERE order_date > DATE '2023-09-17'
      AND channel IS DISTINCT FROM 'web'
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.store_id,
        a.order_date,
        a.status,
        a.channel,
        a.discount,
        a.shipping_fee,
        a.greatest_discount_shipping_fee,
        p.credit_limit,
        p.email,
        p.city
    FROM base a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.delivered_date <= s.order_date
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.carrier = 'DHL'
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY shipping_fee NULLS LAST) AS row_num
    FROM kept
    QUALIFY row_num = 1
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.channel,
    s.city,
    s.row_num,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM latest s
ORDER BY min_account_balance DESC NULLS LAST;
