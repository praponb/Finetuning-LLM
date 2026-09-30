WITH valid AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        CASE WHEN carrier IN ('FedEx', 'UPS') THEN 'Tier 1' ELSE 'Other' END carrier_group,
        DATE_TRUNC('MONTH', shipped_date) shipped_date_period,
        COALESCE(carrier, 'OTHER') carrier_clean
    FROM shipments
    WHERE shipped_date > DATE '2023-08-05'
      AND delivered_date <= '2023-05-01'
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(carrier, TRUE) OVER w_part last_known_carrier,
        FIRST_VALUE(carrier, TRUE) OVER w_running first_known_carrier,
        LAST_VALUE(carrier, TRUE) OVER w_running last_known_carrier_2
    FROM valid
    WINDOW w_part AS (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY order_id ORDER BY shipped_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date_period, id) event_seq,
        FIRST_VALUE(carrier, TRUE) OVER (PARTITION BY carrier ORDER BY shipped_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_carrier_2
    FROM with_windows
),
remaining_keys AS (
    SELECT id, carrier
    FROM metrics
    EXCEPT DISTINCT
    SELECT id, carrier
    FROM shipments
    WHERE delivered_date >= '2024-07-01'
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.carrier = k.carrier
),
enriched AS (
    SELECT
        src.order_id,
        src.carrier,
        src.first_known_carrier,
        src.last_known_carrier_2,
        src.event_seq,
        src.first_known_carrier_2,
        p.shipping_fee
    FROM reconciled src
    INNER JOIN orders p
        ON src.order_id = p.id
    WHERE p.channel = 'mobile'
),
scored AS (
    SELECT
        order_id,
        carrier,
        first_known_carrier,
        last_known_carrier_2,
        event_seq,
        first_known_carrier_2,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY event_seq DESC NULLS LAST) event_seq_cume_dist
    FROM enriched
)
SELECT
    d.channel,
    d.status,
    COUNT(*) AS cnt,
    AVG(s.event_seq) AS avg_event_seq,
    MAX(s.event_seq_cume_dist) AS max_event_seq_cume_dist,
    GROUPING_ID() AS grouping_level
FROM scored s
LEFT JOIN orders d ON s.order_id = d.id
GROUP BY GROUPING SETS ((d.channel, d.status), (d.channel), ())
ORDER BY status DESC NULLS LAST;
