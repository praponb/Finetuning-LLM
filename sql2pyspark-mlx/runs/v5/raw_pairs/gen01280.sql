with store_stats as (
    select store_id, count(*) row_count
    from store_sales
    group by store_id
)
select s.sqft, ss.row_count
from stores s
join store_stats ss on s.id = ss.store_id
where ss.row_count >= 8
order by ss.row_count;
