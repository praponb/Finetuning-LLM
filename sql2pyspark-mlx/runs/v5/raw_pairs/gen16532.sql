WITH valid AS (
    SELECT *, ROUND(units / revenue, 2) AS revenue_per_unit
    FROM store_sales
    WHERE revenue BETWEEN 2500 AND 3200
),
base AS (
    SELECT
        *,
        DAY(sale_date) AS sale_date_day,
        ADD_MONTHS(sale_date, 1) AS sale_date_plus_months
    FROM valid
    WHERE sale_date >= DATE '2023-07-04'
),
remaining_keys AS (
    SELECT store_id, id
    FROM base
    EXCEPT ALL
    SELECT store_id, id
    FROM store_sales
    WHERE units = 34
),
reconciled AS (
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
enriched AS (
    SELECT
        *,
        SUM(units) OVER w_part AS product_id_sum_units,
        STDDEV_SAMP(sale_date_day) OVER w_recent AS store_id_stddev_samp_sale_date_day,
        AVG(revenue_per_unit) OVER w_recent AS store_id_avg_revenue_per_unit
    FROM reconciled
    WINDOW w_part AS (PARTITION BY product_id, store_id), w_recent AS (PARTITION BY store_id)
),
kept_keys AS (
    SELECT product_id, id
    FROM enriched
    EXCEPT ALL
    SELECT product_id, id
    FROM store_sales
    WHERE sale_date <= '2025-06-14'
),
verified AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
joined AS (
    SELECT
        a.id,
        a.product_id,
        a.sale_date,
        a.revenue_per_unit,
        a.sale_date_day,
        a.sale_date_plus_months,
        a.product_id_sum_units,
        a.store_id_stddev_samp_sale_date_day,
        d.cost,
        d.category
    FROM verified a
    LEFT JOIN products d
        ON a.product_id = d.id
    WHERE d.price >= 490
),
stats AS (
    SELECT
        DATE_TRUNC('MONTH', sale_date) AS activity_month,
        ROUND(AVG(cost), 2) AS avg_cost,
        ROUND(SUM(revenue_per_unit) / NULLIF(COUNT(CASE WHEN category = 'Sports' THEN 1 END), 0), 2) AS revenue_per_unit_per_sports
    FROM joined
    GROUP BY DATE_TRUNC('MONTH', sale_date)
    HAVING ROUND(SUM(revenue_per_unit) / NULLIF(COUNT(CASE WHEN category = 'Sports' THEN 1 END), 0), 2) < 1000000
)
SELECT activity_month, revenue_per_unit_per_sports
FROM stats
ORDER BY revenue_per_unit_per_sports DESC
