WITH cleaned AS (
    SELECT
        *,
        DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label,
        ROUND(units * 100, 2) AS units_adjusted
    FROM store_sales
    WHERE sale_date > DATE '2023-12-21'
      AND product_id IN (1, 6, 9)
),
with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sale_date_label,
        units_adjusted,
        SUM(units) OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY units DESC NULLS FIRST) AS units_dense_rank,
        LAG(units_adjusted, 2) OVER (PARTITION BY store_id, product_id ORDER BY sale_date ASC, id ASC) AS prev_units_adjusted,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id, store_id), 4) AS revenue_share
    FROM cleaned
),
with_windows_store_sales AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sale_date_label,
        units_adjusted,
        running_units,
        units_dense_rank,
        prev_units_adjusted,
        revenue_share,
        CORR(revenue, units) OVER (PARTITION BY store_id, product_id) AS corr_revenue_units,
        AVG(prev_units_adjusted) OVER (PARTITION BY product_id) AS product_id_avg_prev_units_adjusted
    FROM with_windows
),
daily AS (
    SELECT
        product_id,
        DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
        store_id,
        BIT_AND(units) AS units_bit_and,
        COUNT(*) AS event_count
    FROM with_windows_store_sales
    GROUP BY product_id, quarter_start, store_id
    HAVING units_bit_and < 1
)
SELECT
    product_id,
    quarter_start,
    store_id,
    units_bit_and,
    event_count
FROM daily
ORDER BY event_count DESC
