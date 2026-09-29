select case when year(ss.sale_date) < 2024 then 'before_2024' when year(ss.sale_date) = 2024 then '2024' else 'after_2024' end as sale_cohort, count(i.quantity) as quantity_count, min(i.last_restock) as min_last_restock
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by case when year(ss.sale_date) < 2024 then 'before_2024' when year(ss.sale_date) = 2024 then '2024' else 'after_2024' end
