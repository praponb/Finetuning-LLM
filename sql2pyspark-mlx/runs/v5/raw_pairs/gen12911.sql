WITH valid AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), delivered_date) AS days_since_delivered_date,
        DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-03-04')
      AND order_id IS NOT NULL
),
enriched AS (
    SELECT
        s.*,
        d.amount,
        d.status AS order_status,
        d.shipping_fee
    FROM valid s
    INNER JOIN orders d
        ON s.order_id <=> d.id
),
metrics AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        days_since_delivered_date,
        days_delivered_date_to_shipped_date,
        amount,
        order_status,
        DENSE_RANK() OVER (ORDER BY cost DESC NULLS LAST) AS cost_dense_rank,
        ROW_NUMBER() OVER (ORDER BY days_delivered_date_to_shipped_date DESC, id) AS event_seq
    FROM enriched
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
)
SELECT
    o.status AS order_status,
    src.status AS shipment_status,
    COUNT(*) AS cnt,
    MAX(src.days_since_delivered_date) AS max_days_since_delivered_date
FROM latest src
INNER JOIN orders o ON src.order_id = o.id
GROUP BY o.status, src.status
ORDER BY order_status DESC NULLS LAST;
