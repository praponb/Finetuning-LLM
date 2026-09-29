with supplier_summary as (
    select supplier_id, sum(case when category = 'Home' then 1 else 0 end) home_count, max(price) max_price
    from products
    group by supplier_id
)
select s.city, ss.home_count, ss.max_price
from suppliers s
left join supplier_summary ss on s.id = ss.supplier_id
