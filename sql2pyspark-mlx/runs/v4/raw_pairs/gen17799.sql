with store_totals as (
    select store_id, max(order_date) as last_order_date
    from orders
    where customer_id is null
    group by store_id
)
select
    s.id,
    s.opened_date,
    s.store_name,
    st.last_order_date
from stores s
left join store_totals st on s.id = st.store_id
order by s.opened_date;
