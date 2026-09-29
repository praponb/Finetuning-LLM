select p.category, count(ss.id) as store_sale_count, min(ss.revenue) as min_revenue from products p inner join store_sales ss on ss.product_id = p.id group by p.category;
