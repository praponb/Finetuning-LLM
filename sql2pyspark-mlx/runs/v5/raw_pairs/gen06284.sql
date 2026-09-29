with ProductsBySupplier as (
    select supplier_id, max(price) as max_price
    from products
    where price between 630 and 1130
    group by supplier_id
)
select s.id, s.rating, P.max_price
from suppliers s
join ProductsBySupplier P on s.id = P.supplier_id
order by s.rating desc, s.id asc;
