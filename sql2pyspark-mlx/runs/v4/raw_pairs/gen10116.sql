WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(carrier) AS carrier_lower
    FROM shipments
    WHERE carrier = 'DHL'
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY delivered_date DESC, id) AS event_seq
FROM base
