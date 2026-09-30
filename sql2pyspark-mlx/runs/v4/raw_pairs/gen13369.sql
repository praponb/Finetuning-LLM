WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        DATE_TRUNC('WEEK', shipped_date) AS shipped_date_period,
        COALESCE(carrier, 'NONE') AS resolved_carrier
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-10-15')
      AND carrier IN ('Flash', 'UPS', 'DHL')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(carrier, TRUE) OVER w_ordered AS first_known_carrier,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS event_seq,
        LAST_VALUE(carrier) IGNORE NULLS OVER w_recent AS last_known_carrier
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY carrier, order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY order_id, carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.amount,
        b.status,
        b.discount
    FROM enriched src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
cleaned_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        shipped_date_period,
        first_known_carrier,
        event_seq,
        amount,
        status,
        discount,
        INITCAP(TRIM(carrier)) AS carrier_title
    FROM enriched_shipments
    WHERE order_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
    CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
    COUNT(*) AS event_count,
    ROUND(AVG(amount), 2) AS avg_amount,
    SUM(CASE WHEN status = 'CANCELLED' THEN event_seq ELSE 0 END) AS cancelled_event_seq
FROM cleaned_shipments
GROUP BY 1, 2;
