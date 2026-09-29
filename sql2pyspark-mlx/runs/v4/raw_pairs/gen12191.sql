select *
from (
    select *, row_number() over (partition by product_id order by last_restock, id) as rank_in_group
    from inventory
) t
where rank_in_group = 1;
