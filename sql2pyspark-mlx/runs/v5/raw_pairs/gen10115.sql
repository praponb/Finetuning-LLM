with store_totals as (
    select store_id, max(quantity) - min(quantity) as quantity_range, count(quantity) as quantity_count
    from inventory
    group by store_id
)
select
    s.id,
    s.city,
    st.quantity_range,
    st.quantity_count
from stores s
inner join store_totals st on s.id = st.store_id
order by quantity_count desc
