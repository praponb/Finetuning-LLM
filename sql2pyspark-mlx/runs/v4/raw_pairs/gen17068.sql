with store_totals as (
    select store_id, max(quantity) as max_quantity, count(*) as total_count
    from inventory
    group by store_id
)
select
    s.city,
    s.store_name,
    st.max_quantity,
    st.total_count
from stores s
inner join store_totals st on s.id = st.store_id
where st.total_count > 7;
