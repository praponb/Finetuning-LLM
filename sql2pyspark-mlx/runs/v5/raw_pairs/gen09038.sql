WITH enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.discount,
        p.status AS order_status
    FROM payments e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.method,
        a.status,
        a.discount,
        o.order_date,
        o.discount AS order_discount,
        o.amount AS order_amount
    FROM enriched a
    JOIN orders o
        ON a.order_id = o.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(order_amount AS INT) AS order_amount_int,
        LAST_DAY(order_date) AS order_date_month_end
    FROM enriched_payments
    WHERE discount < 0.29
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY order_discount DESC, id) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY order_discount DESC NULLS LAST, id) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN order_discount >= 0.35 THEN 'HIGH' ELSE 'LOW' END AS order_discount_tier,
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(order_discount, ARRAY(0.25, 0.5, 0.75)) AS order_discount_quartiles,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT_IF(method = 'card') AS card_events,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    SUM(CASE WHEN status = 'SUCCESS' THEN order_amount_int ELSE 0 END) AS success_order_amount_int
FROM ranked
GROUP BY CASE WHEN order_discount >= 0.35 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', order_date)
HAVING COUNT(*) >= 1
ORDER BY success_order_amount_int NULLS LAST;
