select
    s.region,
    s.city,
    sum(case when o.channel = 'mobile' then o.amount else 0 end) as mobile_amount,
    sort_array(collect_set(o.channel)) as channel_list
from orders o
join stores s on s.id = o.store_id
where o.discount < 0.33
group by s.region, s.city;
