with valid_inventory as (
    select *
    from inventory
    where last_restock < '2024-08-01'
),
last_restock_cohort_summary as (
    select case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end as last_restock_cohort, store_id, max(quantity) as max_quantity
    from valid_inventory
    group by case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end, store_id
)
select last_restock_cohort, store_id, max_quantity
from last_restock_cohort_summary
order by last_restock_cohort desc, store_id
limit 3;
