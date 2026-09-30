select
    s.id,
    s.region,
    t.store_count,
    t.first_order_date
from stores s
left join (select store_id, sum(case when channel = 'store' then 1 else 0 end) as store_count, min(order_date) as first_order_date
    from orders
    group by store_id) t on s.id = t.store_id
where t.store_count > 53
order by t.store_count desc;
