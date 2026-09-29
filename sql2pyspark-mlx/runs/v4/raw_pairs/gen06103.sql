WITH enriched AS (
    SELECT
        *,
        SUM(revenue) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
        FIRST_VALUE(revenue) IGNORE NULLS OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS event_seq,
        LEAD(revenue, 1, 0) OVER (PARTITION BY store_id ORDER BY revenue, id) AS next_revenue
    FROM store_sales
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    revenue,
    first_known_revenue,
    next_revenue
FROM enriched
ORDER BY next_revenue DESC NULLS LAST
