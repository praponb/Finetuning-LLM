with supplier_stats as (
    select supplier_id, sum(case when brand = 'Stark' then price else 0 end) as stark_price
    from products
    where array_contains(tags, 'clearance')
    group by supplier_id
)
select
    s.id,
    s.supplier_name,
    s.city,
    ss.stark_price
from suppliers s
join supplier_stats ss on s.id = ss.supplier_id
where ss.stark_price >= 78
order by ss.stark_price, id desc
limit 5
