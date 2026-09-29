select
    p.id,
    p.price,
    p.stock_qty,
    s.supplier_name,
    s.city,
    s.country,
    concat_ws(',', s.supplier_name, s.city) supplier_name_city_key
from products p
join suppliers s on s.id = p.supplier_id
order by stock_qty desc
