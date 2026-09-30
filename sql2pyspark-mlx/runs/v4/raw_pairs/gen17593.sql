WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        ROUND(revenue, 0) AS revenue_rounded,
        COALESCE(revenue, 0) AS revenue_filled
    FROM store_sales
    WHERE sale_date > DATE '2023-06-29'
),
enriched AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue_rounded,
        revenue_filled,
        FIRST_VALUE(revenue_rounded) IGNORE NULLS OVER w_hist AS first_known_revenue_rounded,
        AVG(revenue_filled) OVER w_part AS store_id_avg_revenue_filled
    FROM cleaned
    WINDOW w_hist AS (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY store_id, product_id)
),
cleaned_store_sales AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue_rounded,
        store_id_avg_revenue_filled,
        revenue_rounded * store_id_avg_revenue_filled AS revenue_rounded_times_store_id_avg_revenue_filled
    FROM enriched
    WHERE revenue_filled >= 38
)
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY product_id ORDER BY store_id_avg_revenue_filled DESC NULLS LAST) AS store_id_avg_revenue_filled_cume_dist,
    SUM(revenue_rounded) OVER (ORDER BY revenue_rounded_times_store_id_avg_revenue_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue_rounded
FROM cleaned_store_sales
