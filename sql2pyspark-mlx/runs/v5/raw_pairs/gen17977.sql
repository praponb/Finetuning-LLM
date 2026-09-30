WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.coupon_codes,
        pos,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts src
    LATERAL VIEW POSEXPLODE(src.items) t AS pos, li_item
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.created_at,
        e.pos,
        e.price,
        c.city,
        c.name
    FROM exploded e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(price) OVER (PARTITION BY city ORDER BY created_at, id, pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price,
        ROW_NUMBER() OVER (ORDER BY price, id, pos) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id, pos) AS seq_no
    FROM joined
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        row_seq + seq_no AS row_seq_plus_seq_no,
        ROUND(row_seq / price, 2) AS row_seq_per_price
    FROM metrics
    WHERE city IS NOT NULL
      AND seq_no = 7
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(created_at) OVER (PARTITION BY customer_id ORDER BY created_at, id, pos) IS NULL
                 OR created_at > LAG(created_at) OVER (PARTITION BY customer_id ORDER BY created_at, id, pos) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY created_at, id, pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN row_seq_plus_seq_no >= 85 THEN 'High' WHEN row_seq_plus_seq_no >= 34 THEN 'Medium' ELSE 'Low' END AS row_seq_plus_seq_no_tier
    FROM islands
    WHERE seq_no BETWEEN 5 AND 7
)
-- Join a dimension and aggregate
SELECT
    d.country,
    a.city AS cart_city,
    COUNT(*) AS cnt,
    AVG(a.row_seq_per_price) AS avg_row_seq_per_price,
    GROUPING_ID() AS gid
FROM base a
INNER JOIN customers d ON a.customer_id = d.id
GROUP BY CUBE (d.country, a.city)
