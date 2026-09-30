WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date
    FROM shipments
    WHERE shipped_date > DATE '2023-06-05'
      AND carrier IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY days_since_shipped_date, id) = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT a.*, b.channel
    FROM ranked a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
ranked_shipments AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS LAST, id) AS rn
    FROM with_dims
    QUALIFY rn = 1
)
-- Join a dimension and aggregate
SELECT
    p.channel AS order_channel,
    s.carrier,
    COUNT(*) AS cnt,
    MAX(s.cost) AS max_cost,
    MAX(s.days_since_shipped_date) AS max_days_since_shipped_date
FROM ranked_shipments s
JOIN orders p ON s.order_id = p.id
GROUP BY GROUPING SETS ((p.channel, s.carrier), (p.channel), ())
ORDER BY max_cost DESC NULLS FIRST
