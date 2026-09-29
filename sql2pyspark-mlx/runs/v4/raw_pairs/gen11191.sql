WITH filtered AS (
    SELECT *, CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END AS is_shipped
    FROM orders
    WHERE discount >= 0.36
),
matched AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM filtered e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.order_date BETWEEN pr.starts_on AND pr.ends_on
),
remaining_keys AS (
    SELECT store_id, customer_id
    FROM matched
    EXCEPT ALL
    SELECT store_id, customer_id
    FROM orders
    WHERE channel NOT IN ('web', 'partner')
),
reconciled AS (
    SELECT s.*
    FROM matched s
    JOIN remaining_keys k ON s.store_id = k.store_id AND s.customer_id = k.customer_id
),
eligible AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.quantity <= 4
        )
)
SELECT
    customer_id,
    order_date,
    status,
    discount,
    shipping_fee,
    promo_id,
    promo_code,
    discount_pct,
    CASE WHEN status RLIKE '^(SHIPPED|PENDING)$' THEN 'CORE' WHEN status RLIKE '^(COMPLETED|CANCELLED)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS status_zone
FROM eligible
ORDER BY discount_pct NULLS FIRST;
