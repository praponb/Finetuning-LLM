WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN carrier IN ('UPS', 'DHL') THEN 'Priority' ELSE 'Other' END AS carrier_group,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE carrier IS NOT DISTINCT FROM 'UPS'
      AND status IN ('DELIVERED', 'LOST')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS seq_no,
        LAST_VALUE(carrier, TRUE) OVER (PARTITION BY order_id, status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        NTILE(4) OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS cost_bucket
    FROM base
),
base_shipments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        status,
        carrier_group,
        status_title,
        carrier_filled,
        seq_no,
        last_known_carrier,
        cost_bucket,
        UPPER(carrier) AS carrier_upper
    FROM enriched
    WHERE shipped_date > DATE '2023-03-02'
      AND order_id IS NOT NULL
),
base_shipments_2 AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN cost >= 105 THEN 'High' WHEN cost >= 15 THEN 'Medium' ELSE 'Low' END AS cost_band,
        ROUND(seq_no * 0.85, 2) AS seq_no_scaled
    FROM base_shipments
    WHERE order_id = 23
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.carrier_group,
        s.status_title,
        s.seq_no,
        s.last_known_carrier,
        s.cost_bucket,
        s.carrier_upper,
        s.cost_band,
        s.seq_no_scaled,
        b.amount,
        b.status AS order_status,
        b.order_date
    FROM base_shipments_2 s
    LEFT JOIN orders b
        ON s.order_id = b.id
    WHERE b.shipping_fee BETWEEN 15 AND 30
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier_group,
        last_known_carrier,
        carrier_upper,
        amount,
        order_status,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY order_status, order_id ORDER BY order_date DESC, id) AS row_seq,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_status), 4) AS amount_share
    FROM with_dims
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_status ORDER BY order_date DESC, id) AS rank_in_group
        FROM with_windows
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    id,
    last_known_carrier,
    amount,
    order_date,
    row_seq,
    rank_in_group
FROM latest;
