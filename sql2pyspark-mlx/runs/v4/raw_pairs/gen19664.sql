with with_window as (
    -- Non-equi join on a validity window
    select
        s.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from sessions s
    left join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.started_at >= sub.start_date
       and s.started_at < sub.end_date + interval 14 days
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (partition by country order by monthly_fee desc nulls last) as monthly_fee_rank,
        row_number() over (partition by browser order by started_at desc, id asc, subscription_id asc) as seq_no,
        last_value(subscription_id) ignore nulls over (partition by plan order by started_at asc, id asc, subscription_id asc rows between unbounded preceding and current row) as last_known_subscription_id,
        cume_dist() over (partition by device, customer_id order by subscription_id desc nulls last) as subscription_id_cume_dist,
        max(monthly_fee) over (partition by customer_id order by cast(started_at as bigint) asc range between 604800 preceding and current row) as monthly_fee_max_7d
    from with_window
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        round(seq_no / sum(seq_no) over (partition by browser, customer_id), 4) as seq_no_share,
        sum(monthly_fee_rank) over (order by seq_no asc, id asc, subscription_id asc rows between unbounded preceding and current row) as running_monthly_fee_rank
    from scored
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time >= s.started_at
        )
),
kept_sessions as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time < s.started_at
        )
),
kept_sessions_2 as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept_sessions s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time < s.started_at
        )
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        b.segment,
        b.credit_limit
    from kept_sessions_2 src
    inner join customers b
        on src.customer_id = b.id
)
select
    id,
    started_at,
    subscription_id,
    monthly_fee,
    monthly_fee_rank,
    last_known_subscription_id,
    subscription_id_cume_dist,
    monthly_fee_max_7d,
    seq_no_share
from joined
