select
    store_id,
    sum(units) as total_units,
    count(*) as cnt,
    sum(sum(units)) over (order by store_id) as cumulative_units
from store_sales
where sale_date between '2023-10-27' and '2024-05-02'
group by store_id
order by 3 desc, 1 asc;
