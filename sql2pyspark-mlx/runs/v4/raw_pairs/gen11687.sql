with supplier_totals as (
    select supplier_id, concat_ws(', ', sort_array(collect_set(category))) as category_values
    from products
    group by supplier_id
)
select
    s.id,
    s.city,
    s.rating,
    st.category_values
from suppliers s
left join supplier_totals st on s.id = st.supplier_id
order by id desc
