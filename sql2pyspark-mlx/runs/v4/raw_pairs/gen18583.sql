WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        CASE WHEN status IN ('SHIPPED', 'COMPLETED') THEN 'Core' ELSE 'Other' END AS status_group
    FROM orders
    WHERE order_date >= TO_DATE('2023-06-11')
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.city,
        p.email,
        p.segment
    FROM cleaned src
    JOIN customers p
        ON src.customer_id = p.id
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM enriched e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date BETWEEN pr.starts_on AND pr.ends_on
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.unit_price,
        top1.order_item_quantity
    FROM attributed s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    customer_id,
    status,
    channel,
    amount,
    discount,
    status_group,
    segment,
    promo_id,
    CASE WHEN status RLIKE '^(SHIPPED|COMPLETED)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS status_zone
FROM with_last
ORDER BY discount DESC;
