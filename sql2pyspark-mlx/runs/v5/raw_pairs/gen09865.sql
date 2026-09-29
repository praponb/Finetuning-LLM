select quantity_bucket, last_restock_quarter, quantity_range
from (
    select case when quantity >= 186 then 'high' when quantity >= 51 then 'mid' else 'low' end as quantity_bucket, quarter(last_restock) as last_restock_quarter, max(quantity) - min(quantity) as quantity_range
    from inventory
    group by case when quantity >= 186 then 'high' when quantity >= 51 then 'mid' else 'low' end, quarter(last_restock)
) as t
where quantity_range >= 24
