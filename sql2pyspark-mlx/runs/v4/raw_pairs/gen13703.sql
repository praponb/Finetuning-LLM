with deduped as (
    select *
    from sessions
    qualify rank() over (partition by customer_id, browser order by started_at nulls first) = 1
),
scored as (
    select
        *,
        row_number() over (partition by device order by started_at desc, id) as seq_no,
        row_number() over (order by started_at desc, id) as seq_no_2
    from deduped
),
enriched as (
    select src.*, p.email
    from scored src
    left join customers p
        on src.customer_id = p.id
),
joined as (
    select
        src.*,
        c.name,
        c.signup_date
    from enriched src
    left join customers c
        on src.customer_id = c.id
),
gaps as (
    select *, case when started_at - lag(started_at) over (partition by country order by started_at, id) > interval '30' day then 1 else 0 end as is_new_burst
    from joined
),
sessionized as (
    select *, sum(is_new_burst) over (partition by country order by started_at, id rows between unbounded preceding and current row) as session_id
    from gaps
),
with_gaps as (
    select *, case when lag(started_at) over (partition by browser order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by browser order by started_at, id) + interval 7 days then 1 else 0 end as is_new_burst_2
    from sessionized
),
islands as (
    select *, sum(is_new_burst_2) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
with_dims as (
    select
        s.*,
        d.credit_limit,
        d.email as customer_email,
        d.signup_date as customer_signup_date
    from islands s
    inner join customers d
        on s.customer_id = d.id
)
select
    session_id,
    min(started_at) as first_seen,
    avg(burst_id) filter (where browser = 'Safari') as safari_avg_burst_id,
    count_if(browser = 'Edge') as edge_events
from with_dims
group by session_id
having edge_events > 1
order by session_id;
