WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        CASE WHEN carrier = 'Flash' THEN 1 ELSE 0 END AS is_flash
    FROM shipments
    WHERE shipped_date >= DATE '2023-05-22'
      AND shipped_date <= '2023-12-01'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY is_flash DESC NULLS LAST) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        DATEDIFF(CURRENT_DATE(), delivered_date) AS days_since_delivered_date
    FROM ranked
    WHERE carrier IS NOT NULL
      AND is_flash >= 35
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.order_date,
        p.channel
    FROM base a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY channel ORDER BY delivered_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_avg_6_prior,
        NTILE(10) OVER (PARTITION BY channel ORDER BY cost DESC, id) AS cost_bucket,
        ROW_NUMBER() OVER (ORDER BY shipped_date, id) AS seq_no,
        RANK() OVER (ORDER BY cost DESC NULLS LAST) AS cost_rank,
        LEAD(cost, 1, 0) OVER (ORDER BY order_date, id) AS next_cost
    FROM with_dims
    WHERE delivered_date > '2025-05-27'
)
SELECT
    id,
    order_id,
    shipped_date,
    delivered_date,
    order_date,
    channel,
    cost_avg_6_prior,
    seq_no,
    cost_rank,
    CASE WHEN channel RLIKE '^(store|web)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS channel_zone
FROM with_windows
