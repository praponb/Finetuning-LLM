with sessions_by_customer as (
    select customer_id, count(case when device = 'desktop' then 1 end) as desktop_count
    from sessions
    where to_date(started_at) < '2025-03-01'
    group by customer_id
)
select c.segment, c.city, sbc.desktop_count
from customers c
left join sessions_by_customer sbc on c.id = sbc.customer_id
where sbc.desktop_count > 4
order by desktop_count, segment desc;
