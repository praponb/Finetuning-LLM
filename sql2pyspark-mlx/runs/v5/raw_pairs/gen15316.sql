with with_latest as (
    select
        s.id,
        s.supplier_name,
        s.country,
        s.rating,
        x.stock_qty,
        x.price
    from suppliers s
    join lateral (
            select c.stock_qty, c.price
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    supplier_name,
    country,
    stock_qty,
    price
from with_latest;
