select
    p.id,
    p.product_name,
    p.cost,
    t.quantity_count
from products p
inner join (select product_id, count(quantity) as quantity_count
    from order_items
    group by product_id) t on p.id = t.product_id
where t.quantity_count > 4
order by cost desc, id
limit 10;
