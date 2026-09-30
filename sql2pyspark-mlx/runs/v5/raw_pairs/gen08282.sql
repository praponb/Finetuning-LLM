WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM shipments
    WHERE order_id IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost ASC) AS cost_dense_rank,
        DENSE_RANK() OVER (PARTITION BY status, order_id ORDER BY cost DESC NULLS LAST) AS cost_dense_rank_2
    FROM valid
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.amount,
        o.status AS order_status
    FROM scored a
    INNER JOIN orders o
        ON a.order_id <=> o.id
    WHERE o.shipping_fee BETWEEN 33 AND 38
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost_dense_rank_2 DESC NULLS LAST, id ASC) = 1
)
-- Grouped metrics
SELECT
    order_status,
    STDDEV_SAMP(cost) AS cost_stddev,
    COUNT(*) FILTER (WHERE order_status = 'SHIPPED') AS shipped_rows
FROM deduped
GROUP BY order_status
