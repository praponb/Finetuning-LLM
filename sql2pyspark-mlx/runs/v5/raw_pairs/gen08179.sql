select
    oi.quantity,
    oi.order_id,
    p.launch_date,
    p.stock_qty,
    s.country,
    s.city,
    coalesce(oi.quantity, 0) as quantity_filled
from order_items oi
left join products p on oi.product_id = p.id
join suppliers s on p.supplier_id = s.id
where s.supplier_name like '%Lion%';
