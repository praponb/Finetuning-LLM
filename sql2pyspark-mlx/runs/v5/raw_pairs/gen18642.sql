WITH enriched AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.sale_date,
        a.revenue,
        p.price,
        p.brand
    FROM store_sales a
    LEFT JOIN products p
        ON a.product_id = p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(revenue) OVER w_running AS next_revenue,
        LEAD(price) OVER w_running AS next_price,
        LAG(price) OVER w_running AS prev_price,
        ROW_NUMBER() OVER w_running AS event_seq,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS revenue_cume_dist
    FROM enriched
    WINDOW w_running AS (PARTITION BY product_id, brand ORDER BY sale_date, id)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.price AS product_price,
        b.category
    FROM metrics e
    LEFT JOIN products b
        ON e.product_id = b.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN prev_price >= 1180 THEN 'High' WHEN prev_price >= 180 THEN 'Medium' ELSE 'Low' END AS prev_price_band
    FROM with_dims
    WHERE sale_date >= TO_DATE('2023-07-24')
      AND category IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        sale_date,
        revenue,
        brand,
        next_revenue,
        next_price,
        prev_price,
        event_seq,
        revenue_cume_dist,
        product_price,
        prev_price_band,
        LEAD(event_seq, 2, 0) OVER (PARTITION BY category, brand ORDER BY prev_price, id) AS next_event_seq,
        LAST_VALUE(next_revenue) IGNORE NULLS OVER w_part AS last_known_next_revenue,
        LAST_VALUE(event_seq) IGNORE NULLS OVER (ORDER BY revenue_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq
    FROM valid
    WINDOW w_part AS (PARTITION BY brand ORDER BY revenue_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Join a dimension and aggregate
SELECT
    d.category,
    d.brand AS product_brand,
    COUNT(*) AS rows_total,
    MAX(src.revenue) AS max_revenue
FROM scored src
INNER JOIN products d ON src.product_id = d.id
GROUP BY ROLLUP (d.category, d.brand)
ORDER BY rows_total DESC NULLS FIRST;
