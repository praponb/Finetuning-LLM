WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(delivered_date, 'yyyy-MM-dd') delivered_date_label,
        CAST(cost AS INT) cost_int
    FROM shipments
    WHERE delivered_date >= DATE '2023-11-21'
      AND carrier IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY carrier, order_id ORDER BY shipped_date DESC) = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(cost, 2) cost_rounded,
        COALESCE(carrier, 'NONE') carrier_filled
    FROM deduped
    WHERE shipped_date > DATE '2023-04-03'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost_int, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
joined AS (
    -- Attach orders attributes
    SELECT s.*, d.order_date
    FROM latest s
    LEFT JOIN orders d
        ON s.order_id = d.id
)
SELECT
    id,
    order_id,
    delivered_date,
    cost,
    cost_int,
    cost_rounded,
    carrier_filled
FROM joined
ORDER BY id, cost_rounded DESC
