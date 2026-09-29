WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(amount AS INT) amount_int,
        COALESCE(method, 'OTHER') resolved_method
    FROM payments
    WHERE paid_at >= DATE '2023-05-29'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id, status
    FROM payments
    WHERE paid_at >= '2023-03-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.status = k.status
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_int,
        resolved_method,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 4) amount_share,
        MAX(amount_int) OVER (PARTITION BY status) status_max_amount_int
    FROM kept_rows
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        amount_int,
        resolved_method,
        amount_share,
        status_max_amount_int,
        DATE_TRUNC('year', paid_at) paid_at_period,
        COALESCE(method, 'UNKNOWN') method_filled
    FROM scored
    WHERE paid_at >= DATE '2023-05-10'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.channel,
        d.discount
    FROM cleaned_payments a
    LEFT JOIN orders d
        ON a.order_id = d.id
)
SELECT
    id,
    order_id,
    paid_at,
    status,
    amount_int,
    amount_share,
    paid_at_period,
    method_filled,
    channel
FROM enriched
ORDER BY status
