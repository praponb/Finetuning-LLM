WITH filtered AS (
    SELECT *, LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE sale_date >= DATE '2023-03-08'
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY units DESC, id) AS latest_rank
FROM filtered
QUALIFY latest_rank = 1
