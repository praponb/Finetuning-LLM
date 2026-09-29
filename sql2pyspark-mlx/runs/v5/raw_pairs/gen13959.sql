select
    s.id,
    s.city,
    t.shipped_amount,
    t.unique_channels
from stores s
left outer join (select store_id, sum(case when status = 'SHIPPED' then amount else 0 end) as shipped_amount, count(distinct amount) as unique_channels
    from orders
    group by store_id) t on s.id = t.store_id
order by id desc;
