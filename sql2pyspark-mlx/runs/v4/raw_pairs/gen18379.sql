WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, customer_id ORDER BY shipping_fee NULLS LAST, id) AS rn
    FROM orders
    QUALIFY rn = 1
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM deduped src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL '14' DAY
),
cleaned AS (
    SELECT
        id,
        store_id,
        order_date,
        status,
        amount,
        shipping_fee,
        promo_id,
        promo_code,
        ROUND(promo_id * 1.1, 2) AS promo_id_adjusted,
        ROUND(amount * 1.07, 2) AS amount_adjusted,
        COALESCE(promo_code, 'UNKNOWN') AS promo_code_clean
    FROM matched
    WHERE order_date > DATE '2023-08-22'
      AND customer_id IS NOT NULL
      AND promo_code IS NOT NULL
)
SELECT
    s.id,
    s.store_id,
    s.order_date,
    s.shipping_fee,
    s.promo_id,
    s.promo_code,
    s.amount_adjusted,
    CASE WHEN promo_code RLIKE '^(BUNDLE|VIP)$' THEN 'GROUP_B' WHEN promo_code RLIKE '^(SPRING10|WELCOME5)$' THEN 'EXTENDED' ELSE 'CORE' END AS promo_code_zone,
    (SELECT MIN(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS min_inventory_quantity
FROM cleaned s;
