select
    dim.region,
    src.status,
    count(*) as num_rows,
    max(src.shipping_fee) as max_shipping_fee,
    max(src.discount) as max_discount
from orders src
inner join stores dim on src.store_id = dim.id
group by dim.region, src.status
order by region desc;
