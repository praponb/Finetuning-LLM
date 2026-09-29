WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        ROUND(unit_price * 1.15, 2) unit_price_adjusted
    FROM order_items
    WHERE quantity != 3
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(unit_price) OVER w_running next_unit_price,
        LEAD(unit_price_adjusted) OVER w_running next_unit_price_adjusted,
        LAG(unit_price_adjusted) OVER w_running prev_unit_price_adjusted,
        ROW_NUMBER() OVER w_running seq_no,
        ROW_NUMBER() OVER w_running event_seq
    FROM filtered
    WINDOW w_running AS (PARTITION BY order_id ORDER BY unit_price_adjusted, id)
),
enriched_order_items AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        unit_price_adjusted,
        next_unit_price,
        next_unit_price_adjusted,
        prev_unit_price_adjusted,
        seq_no,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY order_id), 4) seq_no_share,
        AVG(prev_unit_price_adjusted) OVER (PARTITION BY order_id ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) prev_unit_price_adjusted_avg_2
    FROM enriched
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched_order_items
    EXCEPT ALL
    SELECT id
    FROM order_items
    WHERE order_id IN (13, 29, 37, 40)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_order_items s
    JOIN matched_keys k ON s.id = k.id
)
SELECT
    quantity,
    unit_price,
    unit_price_adjusted,
    next_unit_price,
    next_unit_price_adjusted,
    seq_no,
    prev_unit_price_adjusted_avg_2
FROM verified;
