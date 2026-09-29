WITH filtered AS (
    SELECT *, ADD_MONTHS(sale_date, 3) AS sale_date_plus_months
    FROM store_sales
    WHERE sale_date >= DATE '2023-06-26'
      AND sale_date < '2025-07-01'
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    revenue,
    sale_date_plus_months,
    ROW_NUMBER() OVER (ORDER BY units DESC, id) AS event_seq,
    ROW_NUMBER() OVER w_part AS row_seq,
    LEAD(revenue, 1) OVER (PARTITION BY product_id, store_id ORDER BY revenue, id) AS next_revenue,
    NTILE(10) OVER (PARTITION BY product_id ORDER BY revenue, id) AS revenue_bucket
FROM filtered
WINDOW w_part AS (PARTITION BY store_id, product_id ORDER BY units DESC, id)
