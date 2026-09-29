with store_totals as (
    select store_id, round(stddev(revenue), 2) as stddev_revenue
    from store_sales
    where units >= 35
    group by store_id
)
select s.city, st.stddev_revenue
from stores s
inner join store_totals st on s.id = st.store_id
order by stddev_revenue desc;
