select
    s.city,
    count(p.id) as product_count,
    concat_ws(', ', sort_array(collect_set(p.brand))) as brand_values,
    sum(p.price) as total_price
from suppliers s
left join products p on s.id = p.supplier_id
group by s.city
order by total_price, city;
