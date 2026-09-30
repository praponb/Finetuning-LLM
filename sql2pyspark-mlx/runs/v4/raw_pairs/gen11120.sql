WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.amount,
        b.channel
    FROM order_items a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_dims
    EXCEPT
    SELECT id
    FROM order_items
    WHERE quantity >= 8
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        amount,
        channel,
        RANK() OVER (PARTITION BY product_id ORDER BY amount DESC NULLS LAST) amount_rank,
        CORR(unit_price, quantity) OVER (PARTITION BY channel, order_id) corr_unit_price_quantity,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) unit_price_share,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC) unit_price_dense_rank,
        SUM(amount) OVER (PARTITION BY product_id) product_id_sum_amount
    FROM kept_rows
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        order_id,
        quantity,
        unit_price,
        amount,
        channel,
        amount_rank,
        corr_unit_price_quantity,
        unit_price_share,
        unit_price_dense_rank,
        product_id_sum_amount,
        unit_price_share - amount unit_price_share_minus_amount,
        COALESCE(channel, 'OTHER') channel_filled
    FROM enriched
    WHERE unit_price_dense_rank <> 7
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(unit_price_share_minus_amount) OVER w_running channel_min_unit_price_share_minus_amount,
        RANK() OVER w_hist unit_price_share_minus_amount_rank
    FROM base
    WINDOW w_running AS (PARTITION BY channel), w_hist AS (ORDER BY unit_price_share_minus_amount DESC NULLS FIRST)
)
SELECT
    s.order_id,
    s.quantity,
    s.unit_price,
    s.corr_unit_price_quantity,
    s.unit_price_share,
    s.product_id_sum_amount,
    s.channel_filled,
    s.unit_price_share_minus_amount_rank,
    (SELECT MIN(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS min_shipment_cost
FROM scored s
