SELECT store_id FROM store_sales WHERE revenue >= 2900 UNION ALL SELECT store_id FROM store_sales WHERE units >= 39;
