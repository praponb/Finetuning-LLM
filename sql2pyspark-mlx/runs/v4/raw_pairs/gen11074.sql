select *
from (
    select *, rank() over (partition by supplier_id order by stock_qty desc) as pos
    from products
    where category = 'Home'
) as r
where pos <= 3
order by pos desc;
