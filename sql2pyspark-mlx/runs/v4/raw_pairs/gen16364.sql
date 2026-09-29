WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC NULLS FIRST, id) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cost) OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost,
        MIN(cost) OVER (PARTITION BY carrier, status) carrier_min_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier, status), 4) cost_share,
        ROW_NUMBER() OVER (ORDER BY cost, id) row_seq,
        LEAD(cost, 1) OVER (PARTITION BY carrier ORDER BY delivered_date, id) next_cost
    FROM deduped
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.discount,
        p.amount
    FROM scored src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY next_cost DESC, id) event_seq,
        FIRST_VALUE(cumulative_cost, TRUE) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_cumulative_cost,
        ROUND(cumulative_cost / SUM(cumulative_cost) OVER (PARTITION BY carrier), 4) cumulative_cost_share
    FROM enriched
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched_shipments
    QUALIFY RANK() OVER (PARTITION BY status, order_id ORDER BY shipped_date DESC NULLS LAST) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(first_known_cumulative_cost) first_known_cumulative_cost_whole,
        COALESCE(carrier, 'N/A') carrier_filled
    FROM ranked
    WHERE delivered_date >= DATE '2023-08-24'
      AND order_id IS NOT NULL
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.delivered_date,
    s.status,
    s.cumulative_cost,
    s.carrier_min_cost,
    s.next_cost,
    s.event_seq,
    CASE WHEN carrier RLIKE '^(DHL|Kerry)$' THEN 'CORE' ELSE 'EXTENDED' END AS carrier_zone,
    (SELECT MIN(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS min_payment_amount
FROM filtered s;
