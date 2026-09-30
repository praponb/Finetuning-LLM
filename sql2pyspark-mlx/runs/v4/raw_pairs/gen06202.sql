select s.*, top1.inventory_last_restock
from products s
left join lateral (
        select c.last_restock as inventory_last_restock
        from inventory c
        where c.product_id = s.id
        order by c.last_restock desc, c.id desc
        limit 1
    ) top1 on true
