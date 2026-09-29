WITH joined AS (
    SELECT
        s.*,
        d.amount,
        d.channel
    FROM shipments s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
ranked AS (
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, carrier ORDER BY shipped_date NULLS FIRST, id) = 1
),
deduped AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date NULLS FIRST, id) = 1
),
with_dims AS (
    SELECT src.*, p.discount
    FROM deduped src
    INNER JOIN orders p
        ON src.order_id = p.id
)
SELECT
    id,
    cost,
    amount,
    channel,
    discount,
    CASE WHEN channel RLIKE '^(store|mobile)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS channel_zone
FROM with_dims;
