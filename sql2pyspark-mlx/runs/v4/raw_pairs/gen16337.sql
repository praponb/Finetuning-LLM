WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(revenue, 0) AS revenue_rounded,
        CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE sale_date > DATE '2023-07-12'
      AND store_id IN (4, 5)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(revenue, 1) OVER w_recent AS next_revenue,
        AVG(revenue_rounded) OVER (PARTITION BY store_id ORDER BY units ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS revenue_rounded_avg_5_prior,
        SUM(revenue_rounded) OVER (PARTITION BY product_id ORDER BY sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_rounded,
        LEAD(units, 2) OVER w_running AS next_units,
        CUME_DIST() OVER (PARTITION BY product_id, store_id ORDER BY revenue_rounded DESC NULLS LAST) AS revenue_rounded_cume_dist
    FROM valid
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY units ASC, id ASC), w_running AS (PARTITION BY product_id ORDER BY revenue_rounded ASC, id ASC)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.cost,
        p.brand
    FROM scored src
    LEFT JOIN products p
        ON src.product_id = p.id
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.revenue,
    s.revenue_int,
    s.next_revenue,
    s.cumulative_revenue_rounded,
    s.next_units,
    (SELECT MAX(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS max_inventory_quantity
FROM with_dims s
ORDER BY id DESC
