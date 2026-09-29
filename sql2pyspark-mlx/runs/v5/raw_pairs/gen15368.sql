WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(carrier) carrier_length,
        COALESCE(carrier, 'UNKNOWN') carrier_filled
    FROM shipments
    WHERE shipped_date >= DATE '2023-06-19'
      AND carrier IS DISTINCT FROM 'Flash'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        carrier_filled,
        DATE_TRUNC('WEEK', shipped_date) shipped_date_period,
        CASE WHEN carrier_length >= 67 THEN 'High' WHEN carrier_length >= 16 THEN 'Medium' ELSE 'Low' END carrier_length_tier
    FROM cleaned
    WHERE carrier IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        carrier_filled,
        carrier_length_tier
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date_period DESC, id) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM deduped
    EXCEPT
    SELECT id
    FROM shipments
    WHERE order_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(cost * 0.9, 2) adjusted_cost,
        COALESCE(carrier, 'N/A') carrier_filled_2
    FROM kept_rows
    WHERE carrier = 'FedEx'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY cost DESC NULLS LAST) cost_dense_rank,
        STDDEV_SAMP(adjusted_cost) OVER (PARTITION BY carrier) carrier_stddev_samp_adjusted_cost,
        RANK() OVER (PARTITION BY carrier ORDER BY adjusted_cost DESC) adjusted_cost_rank
    FROM valid
    WHERE cost >= 52
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.channel,
        o.shipping_fee,
        o.status
    FROM metrics src
    JOIN orders o
        ON src.order_id <=> o.id
)
SELECT
    carrier,
    delivered_date,
    carrier_filled,
    adjusted_cost,
    cost_dense_rank,
    carrier_stddev_samp_adjusted_cost,
    channel,
    shipping_fee,
    status
FROM joined
ORDER BY delivered_date, carrier DESC;
