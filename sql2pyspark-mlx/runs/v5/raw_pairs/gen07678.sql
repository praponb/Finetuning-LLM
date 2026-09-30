WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, unit_price + quantity AS line_total
    FROM order_items
    WHERE quantity BETWEEN 4 AND 7
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY line_total DESC, id) AS latest_rank
        FROM filtered
    ) ranked
    WHERE latest_rank = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(line_total, 1) AS line_total_rounded
    FROM latest
    WHERE product_id IS NOT NULL
      AND unit_price < 190
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        latest_rank,
        LAG(unit_price) OVER w_recent AS prev_unit_price,
        LEAD(quantity) OVER w_recent AS next_quantity,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY latest_rank, id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY latest_rank DESC, id) AS row_seq_2,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY next_quantity DESC, id) AS next_quantity_bucket,
        ROW_NUMBER() OVER (ORDER BY unit_price DESC, id) AS event_seq
    FROM enriched
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC NULLS FIRST) AS latest_rank_2
    FROM scored
    QUALIFY latest_rank_2 = 1
),
enriched_order_items AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        prev_unit_price,
        next_quantity,
        event_seq,
        latest_rank_2,
        SUM(row_seq) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        NTILE(3) OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC, id) AS unit_price_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY prev_unit_price, id) AS row_seq_3
    FROM deduped
)
SELECT
    s.id,
    s.order_id,
    s.event_seq,
    s.unit_price_bucket,
    (SELECT SUM(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS sum_shipment_cost
FROM enriched_order_items s;
