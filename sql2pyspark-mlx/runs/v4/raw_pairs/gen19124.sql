with kept_keys as (
    select status, channel
    from orders
    except
    select status, channel
    from orders
    where discount <= 0.1
),
reconciled as (
    select s.*
    from orders s
    inner join kept_keys k on s.status = k.status and s.channel = k.channel
)
select
    case when discount >= 0.38 then 'LARGE' else 'SMALL' end as discount_tier,
    date_trunc('month', order_date) as activity_month,
    count(*) as cnt,
    round(sum(amount) / nullif(count(case when channel = 'partner' then 1 end), 0), 2) as amount_per_partner,
    count_if(status = 'SHIPPED') as shipped_events,
    min(discount) as min_discount,
    grouping_id() as gid
from reconciled
group by rollup (case when discount >= 0.38 then 'LARGE' else 'SMALL' end, date_trunc('month', order_date))
order by cnt;
