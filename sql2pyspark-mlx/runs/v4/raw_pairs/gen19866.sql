WITH base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(unit_price, 0) AS unit_price_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY unit_price DESC, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY unit_price_rounded, id) AS seq_no,
        LEAD(quantity, 1, 0) OVER (ORDER BY unit_price_rounded, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS row_seq,
        SUM(unit_price_rounded) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price_rounded
    FROM base
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY cumulative_unit_price_rounded DESC NULLS FIRST, id) = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.channel,
        d.discount,
        d.amount
    FROM latest e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_rounded,
        event_seq,
        seq_no,
        next_quantity,
        row_seq,
        cumulative_unit_price_rounded,
        channel,
        discount,
        amount,
        AVG(amount) OVER (PARTITION BY order_id) AS order_id_avg_amount,
        FIRST_VALUE(unit_price_rounded, TRUE) OVER (PARTITION BY order_id ORDER BY unit_price_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price_rounded,
        SUM(event_seq) OVER (PARTITION BY order_id, channel ORDER BY next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq
    FROM enriched
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.order_date,
        d.discount AS order_discount,
        d.status
    FROM metrics src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
with_dims_order_items AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.order_date AS order_order_date,
        b.discount AS order_discount_2
    FROM with_dims src
    LEFT JOIN orders b
        ON src.order_id = b.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', order_date) AS year_start,
    SUM(unit_price) AS total_unit_price,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_dims_order_items
GROUP BY DATE_TRUNC('YEAR', order_date)
