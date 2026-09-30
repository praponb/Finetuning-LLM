select case when year(delivered_date) < 2024 then 'before_2024' when year(delivered_date) = 2024 then '2024' else 'after_2024' end as delivered_cohort, sum(cost) as total_cost, rank() over (order by sum(cost) desc) as cost_rank
from shipments
where order_id = 19
group by case when year(delivered_date) < 2024 then 'before_2024' when year(delivered_date) = 2024 then '2024' else 'after_2024' end
order by 3 asc
