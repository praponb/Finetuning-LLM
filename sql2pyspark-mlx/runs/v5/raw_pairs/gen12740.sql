select
    s.*,
    latest.stock_qty,
    latest.category
from suppliers s
left join lateral (
        select c.stock_qty, c.category
        from products c
        where c.supplier_id = s.id
        order by c.cost desc, c.id desc
        limit 1
    ) latest on true
