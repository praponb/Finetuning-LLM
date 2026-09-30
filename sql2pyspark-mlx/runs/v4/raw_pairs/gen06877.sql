WITH scored AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        NTILE(3) OVER (ORDER BY revenue DESC, id) AS revenue_bucket,
        LEAD(units, 1) OVER w_hist AS next_units
    FROM store_sales
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY revenue, id)
),
enriched AS (
    SELECT
        s.*,
        p.region,
        p.store_name,
        p.opened_date
    FROM scored s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
kept_keys AS (
    SELECT store_id
    FROM enriched
    EXCEPT
    SELECT store_id
    FROM store_sales
    WHERE product_id = 1
),
verified AS (
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.store_id = k.store_id
),
filtered AS (
    SELECT *, COALESCE(units, 0) AS units_filled
    FROM verified
    WHERE sale_date >= DATE '2023-08-21'
      AND region IS NOT NULL
      AND MONTH(sale_date) IN (2, 8, 12)
),
with_dims AS (
    SELECT src.*, p.product_name
    FROM filtered src
    JOIN products p
        ON src.product_id = p.id
),
enriched_store_sales AS (
    SELECT s.*, p2.opened_date AS store_opened_date
    FROM with_dims s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
totals AS (
    SELECT
        CASE WHEN units >= 31 THEN 'LARGE' ELSE 'SMALL' END AS units_tier,
        CORR(revenue, units) AS corr_revenue_units,
        SUM(revenue) AS total_revenue,
        PERCENTILE_APPROX(revenue, 0.95) AS revenue_p95
    FROM enriched_store_sales
    GROUP BY CASE WHEN units >= 31 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    units_tier,
    total_revenue,
    revenue_p95
FROM totals
ORDER BY total_revenue DESC;
