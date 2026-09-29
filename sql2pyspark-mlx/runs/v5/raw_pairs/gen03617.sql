select case when o.channel in ('mobile', 'partner') then 'tier_1' else 'other' end as channel_group, count(s.cost) as cost_count, max(s.shipped_date) as last_shipped_date
from shipments s
inner join orders o on o.id = s.order_id
inner join stores st on st.id = o.store_id
group by case when o.channel in ('mobile', 'partner') then 'tier_1' else 'other' end
order by 2 desc;
