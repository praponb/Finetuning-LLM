with supplier_totals as (
    select supplier_id, round(stddev(price), 2) as stddev_price
    from products
    group by supplier_id
)
select s.country, s.supplier_name, st.stddev_price
from suppliers s
left outer join supplier_totals st on s.id = st.supplier_id
order by st.stddev_price;
