WITH joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.launch_date,
        d.category
    FROM order_items s
    INNER JOIN products d
        ON s.product_id <=> d.id
    WHERE MONTH(d.launch_date) IN (3, 10, 11)
),
joined_order_items AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.unit_price,
        e.category,
        b.channel,
        b.amount,
        b.discount
    FROM joined e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(channel) channel_lower,
        ROUND(unit_price * 0.9, 2) unit_price_adjusted
    FROM joined_order_items
    WHERE category IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY discount DESC NULLS LAST, id) = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT src.*, b.channel AS order_channel
    FROM deduped src
    INNER JOIN orders b
        ON src.order_id = b.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        unit_price,
        channel,
        amount,
        order_channel,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY unit_price_adjusted DESC NULLS LAST) unit_price_adjusted_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY order_channel ORDER BY unit_price_adjusted, id) seq_no
    FROM enriched
)
SELECT
    id,
    channel,
    amount,
    order_channel,
    unit_price_adjusted_cume_dist,
    seq_no
FROM metrics
ORDER BY channel DESC NULLS FIRST, id DESC;
