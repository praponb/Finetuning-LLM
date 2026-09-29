WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DAY(sale_date) AS sale_date_day,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE units >= 22
)
-- Window metrics over several different partitions
SELECT
    id,
    store_id,
    revenue,
    LEAD(sale_date_day) OVER w_hist AS next_sale_date_day,
    LAG(sale_date_day) OVER w_hist AS prev_sale_date_day,
    LEAD(revenue) OVER w_hist AS next_revenue,
    ROW_NUMBER() OVER w_hist AS row_seq,
    FIRST_VALUE(units) IGNORE NULLS OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units
FROM valid
WHERE product_id = 3
WINDOW w_hist AS (PARTITION BY product_id, store_id ORDER BY sale_date_day, id);
