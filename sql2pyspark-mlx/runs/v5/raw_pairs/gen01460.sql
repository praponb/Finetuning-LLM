WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM payments
    WHERE paid_at > DATE '2023-09-28'
      AND order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.method,
        a.paid_at,
        a.status,
        b.shipping_fee
    FROM cleaned a
    LEFT JOIN orders b
        ON a.order_id <=> b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        paid_at,
        status,
        UPPER(status) AS status_upper
    FROM with_dims
    WHERE method IS DISTINCT FROM 'cash'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY paid_at ASC NULLS FIRST, id ASC) = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.status,
        d.order_date
    FROM deduped src
    INNER JOIN orders d
        ON src.order_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, ROW_NUMBER() OVER (ORDER BY order_date ASC, id ASC) AS row_seq
    FROM enriched
)
SELECT
    id,
    order_date,
    row_seq
FROM metrics
