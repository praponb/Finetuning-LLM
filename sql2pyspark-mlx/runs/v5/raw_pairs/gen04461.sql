WITH filtered AS (
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-05-01')
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS rank_in_group
        FROM filtered
    ) t
    WHERE rank_in_group = 1
),
enriched AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        rank_in_group,
        LEAD(cost, 2) OVER (PARTITION BY status ORDER BY cost, id) AS next_cost,
        LAST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY carrier, status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        LAG(cost, 1, 0) OVER (PARTITION BY status ORDER BY shipped_date, id) AS prev_cost,
        NTILE(10) OVER (PARTITION BY status, order_id ORDER BY cost, id) AS cost_bucket
    FROM ranked
),
enriched_shipments AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_bucket DESC, id) AS event_seq,
        RANK() OVER (PARTITION BY carrier, order_id ORDER BY prev_cost NULLS LAST) AS prev_cost_rank,
        RANK() OVER (ORDER BY cost_bucket NULLS LAST) AS cost_bucket_rank
    FROM enriched
),
filtered_shipments AS (
    SELECT
        *,
        COALESCE(carrier, 'N/A') AS carrier_filled,
        GREATEST(event_seq, next_cost) AS greatest_event_seq_next_cost,
        COALESCE(carrier, 'NONE') AS carrier_filled_2
    FROM enriched_shipments
    WHERE delivered_date >= DATE '2023-02-26'
      AND carrier IS NOT NULL
),
enriched_shipments_2 AS (
    SELECT s.*, b.discount
    FROM filtered_shipments s
    LEFT JOIN orders b
        ON s.order_id = b.id
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.rank_in_group,
    s.prev_cost,
    s.cost_bucket,
    s.event_seq,
    s.prev_cost_rank,
    (SELECT MIN(o2.quantity) FROM order_items o2 WHERE o2.order_id = s.order_id) AS min_order_item_quantity
FROM enriched_shipments_2 s
ORDER BY rank_in_group DESC NULLS LAST, id;
