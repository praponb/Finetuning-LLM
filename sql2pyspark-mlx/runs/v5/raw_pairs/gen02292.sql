SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC NULLS FIRST, id) AS latest_rank
FROM store_sales
QUALIFY latest_rank = 1
