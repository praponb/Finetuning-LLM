with supplier_totals as (
    select supplier_id, max(launch_date) as last_launch_date, min(launch_date) as min_launch_date
    from products
    group by supplier_id
)
select
    s.id,
    s.city,
    s.rating,
    st.last_launch_date,
    st.min_launch_date
from suppliers s
inner join supplier_totals st on s.id = st.supplier_id
order by rating desc
