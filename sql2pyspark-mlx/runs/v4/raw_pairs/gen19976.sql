select case when year(s.opened_date) < 2024 then 'before_2024' when year(s.opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, max(i.last_restock) as last_last_restock, min(i.last_restock) as min_last_restock
from inventory i
inner join stores s on i.store_id = s.id
full outer join products p on i.product_id = p.id
where s.sqft = 10159
group by case when year(s.opened_date) < 2024 then 'before_2024' when year(s.opened_date) = 2024 then '2024' else 'after_2024' end
order by opened_cohort desc, min_last_restock;
