WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(carrier, 'N/A') AS carrier_filled,
        CONCAT(status, ' ', carrier) AS status_carrier
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-08-05')
      AND carrier IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        o.order_date,
        o.channel,
        o.amount
    FROM base e
    LEFT JOIN orders o
        ON e.order_id = o.id
    WHERE o.order_date > '2024-04-01'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        carrier_filled,
        status_carrier,
        channel,
        ROUND(amount, 0) AS amount_rounded,
        DATE_SUB(delivered_date, 30) AS delivered_date_minus_30d
    FROM enriched
    WHERE delivered_date > TO_DATE('2023-03-18')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status_carrier,
        channel,
        delivered_date_minus_30d,
        LEAD(amount_rounded) OVER w_ordered AS next_amount_rounded,
        LAG(cost) OVER w_ordered AS prev_cost,
        LEAD(cost) OVER w_ordered AS next_cost,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        NTILE(10) OVER w_running AS amount_rounded_bucket
    FROM valid
    WINDOW w_ordered AS (PARTITION BY channel ORDER BY shipped_date ASC, id ASC), w_running AS (PARTITION BY order_id ORDER BY amount_rounded ASC, id ASC)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY next_amount_rounded ASC, id ASC) = 1
)
-- Join a dimension and aggregate
SELECT
    p.status,
    s.channel AS shipment_channel,
    COUNT(*) AS cnt,
    MAX(s.cost) AS max_cost
FROM ranked s
LEFT JOIN orders p ON s.order_id = p.id
GROUP BY GROUPING SETS ((p.status, s.channel), (p.status), ())
ORDER BY max_cost ASC;
