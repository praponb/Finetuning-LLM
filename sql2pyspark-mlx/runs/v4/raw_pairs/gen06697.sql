WITH filtered AS (
    SELECT *, LEAST(units, revenue) AS least_units_revenue
    FROM store_sales
    WHERE sale_date >= TO_DATE('2024-03-27')
      AND revenue >= 3800
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        least_units_revenue,
        SUM(revenue) OVER w_hist AS store_id_sum_revenue,
        NTILE(10) OVER (ORDER BY units DESC, id) AS units_bucket,
        CORR(units, revenue) OVER w_grp AS corr_units_revenue,
        COUNT(least_units_revenue) OVER w_grp AS store_id_count_least_units_revenue,
        SUM(units) OVER (PARTITION BY product_id ORDER BY least_units_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units
    FROM filtered
    WINDOW w_hist AS (PARTITION BY store_id, product_id), w_grp AS (PARTITION BY store_id)
),
scored AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        least_units_revenue,
        store_id_sum_revenue,
        units_bucket,
        corr_units_revenue,
        store_id_count_least_units_revenue,
        cumulative_units,
        AVG(corr_units_revenue) OVER (PARTITION BY product_id ORDER BY cumulative_units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS corr_units_revenue_avg_6,
        ROW_NUMBER() OVER (ORDER BY store_id_sum_revenue, id) AS seq_no
    FROM metrics
)
SELECT *
FROM (
    SELECT
        id,
        store_id,
        least_units_revenue,
        revenue,
        corr_units_revenue
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (least_units_revenue, revenue, corr_units_revenue)
);
