with exploded as (
    select
        id,
        customer_id,
        created_at,
        key,
        attr_value
    from carts
    lateral view explode(attributes) li as key, attr_value
),
scored as (
    select *, row_number() over (order by created_at, id, key) as event_seq
    from exploded
    where key = 'utm_source'
),
matched as (
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from scored src
    inner join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.created_at between sub.start_date and sub.end_date
),
ranked as (
    select *
    from matched
    qualify row_number() over (partition by plan order by monthly_fee desc nulls first, id, key, subscription_id) = 1
),
enriched as (
    select
        *,
        count(event_seq) over (partition by plan order by event_seq, id, key, subscription_id rows between 3 preceding and current row) as last4_count_event_seq,
        avg(event_seq) over (partition by plan order by event_seq, id, key, subscription_id rows between 3 preceding and current row) as last4_avg_event_seq,
        sum(event_seq) over (partition by plan order by event_seq, id, key, subscription_id rows between 3 preceding and current row) as last4_sum_event_seq,
        min(event_seq) over (partition by plan order by event_seq, id, key, subscription_id rows between 3 preceding and current row) as last4_min_event_seq,
        lead(subscription_id, 2) over (partition by key order by event_seq, id, key, subscription_id) as next_subscription_id
    from ranked
)
select
    p.country,
    e.key,
    count(*) as num_rows,
    max(e.last4_sum_event_seq) as max_last4_sum_event_seq
from enriched e
left join customers p on e.customer_id = p.id
group by p.country, e.key
order by num_rows desc nulls first;
