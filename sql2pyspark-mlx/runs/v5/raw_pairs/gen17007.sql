with filtered as (
    -- Basic cleansing and defaults
    select *, amount - shipping_fee as amount_with_shipping
    from orders
    where order_date > date '2023-02-24'
)
-- Join a dimension and aggregate
select
    d.region,
    d.city,
    a.status,
    count(*) as rows_total,
    min(a.discount) as min_discount,
    max(a.amount) as max_amount
from filtered a
left join stores d on a.store_id = d.id
group by cube (d.region, d.city, a.status)
order by min_discount nulls first
