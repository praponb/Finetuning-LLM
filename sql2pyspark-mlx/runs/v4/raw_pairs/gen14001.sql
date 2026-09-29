WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
    FROM payments
    WHERE TO_DATE(paid_at) < '2025-05-24'
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.amount AS order_amount,
        d.order_date,
        d.status AS order_status
    FROM valid e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.discount,
        o.order_date AS order_order_date,
        o.status AS order_status_2
    FROM joined src
    LEFT JOIN orders o
        ON src.order_id = o.id
)
SELECT
    id,
    method,
    status,
    days_since_paid_at,
    order_amount,
    order_date,
    order_status,
    discount,
    order_order_date,
    CASE WHEN status RLIKE '^(FAILED|SUCCESS)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(REFUNDED)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS status_zone
FROM enriched;
