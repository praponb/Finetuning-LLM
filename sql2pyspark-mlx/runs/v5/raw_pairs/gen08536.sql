SELECT
    id,
    store_id,
    units,
    revenue,
    LAG(units) OVER w_ordered AS prev_units,
    LAG(revenue) OVER w_ordered AS prev_revenue,
    LEAD(revenue) OVER w_ordered AS next_revenue,
    ROW_NUMBER() OVER w_ordered AS row_seq
FROM store_sales
WINDOW w_ordered AS (PARTITION BY product_id ORDER BY units ASC, id ASC);
