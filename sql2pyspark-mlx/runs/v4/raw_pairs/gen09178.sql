with latest as (
    select *
    from sessions
    qualify row_number() over (partition by browser, country order by started_at desc nulls first, id) = 1
),
with_windows as (
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        row_number() over (partition by customer_id order by started_at desc, id) as event_seq,
        row_number() over (partition by device order by started_at, id) as seq_no,
        row_number() over (partition by browser order by started_at, id) as seq_no_2
    from latest
),
joined as (
    select e.*, b.city
    from with_windows e
    left join customers b
        on e.customer_id = b.id
),
with_gaps as (
    select *, case when started_at - lag(started_at) over (partition by customer_id order by started_at, id) > interval 90 days then 1 else 0 end as is_new_session
    from joined
),
sessions_sessions as (
    select *, sum(is_new_session) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
select
    id,
    customer_id,
    device,
    browser,
    country,
    seq_no_2,
    is_new_session,
    island_id
from sessions_sessions
order by seq_no_2 desc, is_new_session;
