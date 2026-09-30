WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        status,
        TRIM(method) AS clean_method
    FROM payments
    WHERE paid_at > DATE '2023-11-01'
      AND method IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM cleaned
    EXCEPT ALL
    SELECT status, id
    FROM payments
    WHERE status != 'FAILED'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.channel,
        d.discount,
        d.status AS order_status
    FROM kept_rows e
    LEFT JOIN orders d
        ON e.order_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN discount >= 0.45 THEN 'HIGH' ELSE 'LOW' END AS discount_tier,
    SUM(discount) FILTER (WHERE status = 'REFUNDED') AS refunded_total_discount,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MAX(discount) FILTER (WHERE order_status = 'PENDING') AS pending_max_discount
FROM enriched
GROUP BY 1;
