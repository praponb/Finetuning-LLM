with store_totals as (
    select store_id, count(*) as row_count, max(units) - min(units) as units_range
    from store_sales
    where product_id in (1, 3)
    group by store_id
)
select
    s.id,
    s.sqft,
    s.region,
    st.row_count,
    st.units_range
from stores s
join store_totals st on s.id = st.store_id
where st.row_count > 6;
