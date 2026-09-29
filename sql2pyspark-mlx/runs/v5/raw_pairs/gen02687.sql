select
    store_id,
    last_restock_cohort,
    avg_quantity,
    num_inventory
from (
    select
        store_id,
        case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end as last_restock_cohort,
        avg(quantity) as avg_quantity,
        count(*) as num_inventory
    from inventory
    group by store_id, case when year(last_restock) < 2024 then 'before_2024' when year(last_restock) = 2024 then '2024' else 'after_2024' end
) as t
where avg_quantity < 180
order by store_id desc;
