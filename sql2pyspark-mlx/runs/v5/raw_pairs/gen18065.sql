with supplier_totals as (
    select supplier_id, min(launch_date) as min_launch_date
    from products
    group by supplier_id
)
select s.rating, s.city, st.min_launch_date
from suppliers s
left join supplier_totals st on s.id = st.supplier_id
order by rating;
