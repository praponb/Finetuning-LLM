WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        COALESCE(amount, 0) AS amount_filled_2,
        COALESCE(method, 'N/A') AS method_clean
    FROM payments
    WHERE method IS NOT NULL
      AND method IN ('card', 'paypal', 'promptpay')
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.method,
        s.status,
        s.amount_filled_2,
        p.amount AS order_amount,
        p.discount
    FROM filtered s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        status,
        order_amount,
        discount,
        CASE WHEN method = 'cash' THEN 1 ELSE 0 END AS is_cash,
        COALESCE(method, 'OTHER') AS method_filled
    FROM joined
    WHERE method IS DISTINCT FROM 'cash'
      AND status <> 'REFUNDED'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT order_id
    FROM base
    INTERSECT
    SELECT order_id
    FROM payments
    WHERE TO_DATE(paid_at) < '2024-01-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(order_amount) AS order_amount_whole,
        discount - is_cash AS discount_minus_is_cash,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM reconciled
    WHERE method IS NOT NULL
      AND discount < 0.2
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_cash >= 73 THEN 'HIGH' ELSE 'LOW' END AS is_cash_tier,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count,
    SUM(order_amount_whole) FILTER (WHERE method = 'card') AS card_total_order_amount_whole,
    COUNT(*) AS row_count
FROM cleaned
GROUP BY 1;
