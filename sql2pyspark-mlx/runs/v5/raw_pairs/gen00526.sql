WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        CEIL(amount) AS amount_whole
    FROM orders
    WHERE amount BETWEEN 800 AND 870
      AND order_date >= DATE '2023-03-14'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM filtered e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date >= pr.starts_on
       AND e.order_date < pr.ends_on + INTERVAL '30' DAY
),
with_latest AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.cost,
        latest.shipped_date
    FROM matched s
    LEFT JOIN LATERAL (
            SELECT c.cost, c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.order_date,
        e.discount,
        e.amount_whole,
        e.promo_id,
        e.promo_code,
        e.discount_pct,
        d.store_name,
        d.region,
        d.city
    FROM with_latest e
    LEFT JOIN stores d
        ON e.store_id = d.id
    WHERE d.sqft > 11756
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.amount_whole,
        src.promo_id,
        src.discount_pct,
        src.store_name,
        src.region,
        p.sqft
    FROM joined src
    LEFT JOIN stores p
        ON src.store_id = p.id
)
SELECT
    id,
    store_id,
    promo_id,
    store_name,
    region,
    sqft
FROM enriched
ORDER BY sqft DESC, store_id DESC
