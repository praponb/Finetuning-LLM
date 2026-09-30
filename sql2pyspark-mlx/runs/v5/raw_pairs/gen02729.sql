select p.brand, min(ss.sale_date) as min_sale_date from store_sales ss inner join products p on p.id = ss.product_id group by p.brand order by brand
