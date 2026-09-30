WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        status,
        COALESCE(amount, 0) AS amount_filled,
        CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END AS is_bank_transfer,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-07-27 09:00:00')
      AND amount >= 540
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount_filled NULLS LAST, id) = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT order_id, status
    FROM ranked
    INTERSECT ALL
    SELECT order_id, status
    FROM payments
    WHERE order_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.status = k.status
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.resolved_method,
        d.discount
    FROM verified s
    INNER JOIN orders d
        ON s.order_id = d.id
    WHERE d.store_id IS NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN discount >= 0.36 THEN 'High' WHEN discount >= 0.06 THEN 'Medium' ELSE 'Low' END AS discount_level
    FROM with_dims
    WHERE discount < 0.35
),
stats AS (
    -- Grouped metrics
    SELECT
        order_id,
        ROUND(AVG(discount), 2) AS avg_discount,
        COUNT(*) AS cnt
    FROM cleaned
    GROUP BY 1
)
SELECT
    order_id,
    avg_discount,
    cnt
FROM stats
ORDER BY cnt DESC NULLS LAST;
