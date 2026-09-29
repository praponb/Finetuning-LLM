select
    s.id,
    s.city,
    count(p.id) product_count,
    sum(p.price) total_price,
    sum(case when p.category = 'Books' then p.price else 0 end) books_price
from suppliers s
left join products p on s.id = p.supplier_id
group by s.id, s.city
having total_price >= 6300;
