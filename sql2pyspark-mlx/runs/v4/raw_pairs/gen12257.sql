select
    store_id,
    sum(case when status = 'CANCELLED' then amount else 0 end) as cancelled_amount,
    concat_ws(', ', sort_array(collect_set(channel))) as channel_values,
    sort_array(collect_set(channel)) as channel_list
from orders
where amount > 1500
group by store_id
having sum(case when status = 'CANCELLED' then amount else 0 end) < 60
order by cancelled_amount;
