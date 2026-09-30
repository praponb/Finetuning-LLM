WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY delivered_date, id) AS dedup_rank
        FROM shipments
    ) x
    WHERE dedup_rank = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        o.channel,
        o.discount
    FROM deduped s
    INNER JOIN orders o
        ON s.order_id = o.id
),
with_dims_shipments AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.status AS order_status,
        p.amount
    FROM with_dims a
    INNER JOIN orders p
        ON a.order_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS event_seq,
        AVG(discount) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_avg_5_prior
    FROM with_dims_shipments
),
with_dims_shipments_2 AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.status AS order_status_2,
        p.discount AS order_discount,
        p.order_date
    FROM scored s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
enriched AS (
    -- Attach orders attributes
    SELECT e.*, o.discount AS order_discount_2
    FROM with_dims_shipments_2 e
    LEFT JOIN orders o
        ON e.order_id = o.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_avg_5_prior DESC NULLS LAST, id) = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        order_id,
        order_discount,
        discount,
        amount
    FROM latest
) src
UNPIVOT (
    reading FOR metric IN (order_discount AS ORDER_DISCOUNT_VAL, discount AS DISCOUNT_VAL, amount AS AMOUNT_VAL)
);
