select
    s.id,
    s.product_name,
    s.category,
    s.price,
    s.cost,
    s.tags,
    latest.quantity,
    latest.last_restock
from products s
left join lateral (
        select c.quantity, c.last_restock
        from inventory c
        where c.product_id = s.id
          and c.last_restock <= s.launch_date
        order by c.last_restock desc, c.id desc
        limit 1
    ) latest on true;
