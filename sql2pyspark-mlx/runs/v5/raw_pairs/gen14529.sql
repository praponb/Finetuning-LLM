WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        LAST_DAY(shipped_date) AS shipped_date_month_end
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-09-05')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date_month_end NULLS FIRST, id) AS row_num
        FROM filtered
    ) x
    WHERE row_num = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.cost,
        p.channel
    FROM ranked src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT src.*, p.amount
    FROM enriched src
    LEFT JOIN orders p
        ON src.order_id = p.id
    WHERE p.channel = 'store'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY amount) AS rn
    FROM enriched_shipments
    QUALIFY rn = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        cost,
        channel,
        amount,
        NTILE(10) OVER (PARTITION BY channel, order_id ORDER BY cost DESC, id) AS cost_bucket,
        PERCENT_RANK() OVER (PARTITION BY order_id, channel ORDER BY cost NULLS LAST) AS cost_percent_rank
    FROM latest
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, amount - cost_percent_rank AS amount_minus_cost_percent_rank
    FROM metrics
    WHERE order_id IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'store'
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    channel,
    metric_name,
    val
FROM (
    SELECT
        id,
        channel,
        amount,
        cost
    FROM cleaned
) src
UNPIVOT (
    val FOR metric_name IN (amount, cost)
);
