select
    s.id,
    s.country,
    s.city,
    top1.product_brand
from suppliers s
join lateral (
        select c.brand as product_brand
        from products c
        where c.supplier_id = s.id
        order by c.stock_qty desc, c.id desc
        limit 1
    ) top1 on true
