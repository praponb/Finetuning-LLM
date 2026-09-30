select id, category, stock_qty
from (
    select
        id,
        category,
        stock_qty,
        rank() over (partition by category order by stock_qty desc) rn
    from products
    where cost between 300 and 520
) r
where rn <= 2
