with supplier_totals as (
    select supplier_id, count(case when brand = 'Globex' then 1 end) as globex_count
    from products
    group by supplier_id
)
select s.city, st.globex_count
from suppliers s
join supplier_totals st on s.id = st.supplier_id
order by city desc
