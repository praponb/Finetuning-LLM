SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS rank_in_group FROM store_sales QUALIFY rank_in_group = 1;
