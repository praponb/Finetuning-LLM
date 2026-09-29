with store_totals as (
    select store_id, max(revenue) - min(revenue) revenue_range
    from store_sales
    group by store_id
)
select
    s.id,
    s.opened_date,
    s.region,
    st.revenue_range
from stores s
left join store_totals st on s.id = st.store_id
order by s.opened_date;
