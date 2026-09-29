select s.*, top1.unit_price
from products s
left join lateral (
        select c.unit_price
        from order_items c
        where c.product_id = s.id
        order by c.quantity desc, c.id desc
        limit 1
    ) top1 on true;
