WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(unit_price * 1.15, 2) AS adjusted_unit_price
    FROM order_items
    WHERE order_id IN (3, 16, 19)
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.quantity,
        o.amount,
        o.channel
    FROM valid src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(amount) AS amount_whole
    FROM joined
    WHERE channel IS NOT NULL
      AND order_id = 16
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        amount,
        amount_whole,
        dedup_rank,
        amount_whole - amount AS amount_whole_minus_amount,
        LEAST(quantity, dedup_rank) AS least_quantity_dedup_rank
    FROM latest
    WHERE channel IS NOT NULL
      AND quantity >= 7
)
-- Join a dimension and aggregate
SELECT
    dim.channel,
    dim.status,
    COUNT(*) AS rows_total,
    ROUND(SUM(src.least_quantity_dedup_rank), 2) AS total_least_quantity_dedup_rank,
    SUM(src.amount_whole_minus_amount) AS total_amount_whole_minus_amount,
    GROUPING_ID() AS grouping_level
FROM cleaned src
INNER JOIN orders dim ON src.order_id = dim.id
GROUP BY GROUPING SETS ((dim.channel, dim.status), (dim.channel), ());
