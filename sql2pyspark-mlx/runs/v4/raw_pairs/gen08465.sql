select
    s.*,
    top1.units,
    top1.sale_date
from products s
left join lateral (
        select c.units, c.sale_date
        from store_sales c
        where c.product_id = s.id
          and c.sale_date <= s.launch_date
        order by c.sale_date desc, c.id desc
        limit 1
    ) top1 on true
