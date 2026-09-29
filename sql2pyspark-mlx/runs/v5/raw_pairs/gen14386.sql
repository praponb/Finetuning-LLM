select s.sqft, ss.sale_date, ss.revenue, ss.product_id from stores s left join store_sales ss on s.id = ss.store_id where s.region <> 'West' and lower(s.store_name) like '%iam%';
