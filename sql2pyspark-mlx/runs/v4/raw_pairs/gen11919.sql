WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        units * revenue revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-03-20'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date NULLS LAST, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        MIN(revenue) OVER w_ordered store_id_min_revenue,
        NTILE(3) OVER w_ordered_2 revenue_per_unit_bucket
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY store_id), w_ordered_2 AS (PARTITION BY store_id ORDER BY revenue_per_unit DESC, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id_min_revenue, id) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, MONTH(sale_date) sale_date_month
    FROM deduped
    WHERE sale_date > TO_DATE('2023-06-25')
),
deduped_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue_per_unit DESC NULLS FIRST, id) AS row_num_2
        FROM base
    ) x
    WHERE row_num_2 = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY revenue_per_unit DESC, id) revenue_per_unit_bucket_2,
        LEAD(revenue, 2) OVER (PARTITION BY store_id ORDER BY sale_date, id) next_revenue,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) units_share,
        AVG(store_id_min_revenue) OVER (PARTITION BY product_id ORDER BY store_id_min_revenue, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) store_id_min_revenue_avg_2
    FROM deduped_store_sales
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    CASE WHEN revenue >= 3100 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    store_id,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    ROUND(AVG(units_share), 2) AS avg_units_share,
    CORR(units, revenue) AS corr_units_revenue,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    RANK() OVER (PARTITION BY product_id ORDER BY STDDEV_SAMP(revenue) DESC) AS revenue_stddev_rank
FROM scored
GROUP BY product_id, CASE WHEN revenue >= 3100 THEN 'PREMIUM' ELSE 'STANDARD' END, store_id
ORDER BY distinct_store_ids DESC;
