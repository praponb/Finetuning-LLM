with eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from sessions s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time < s.started_at
        )
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        first_value(country, true) over (partition by country order by started_at, id rows between unbounded preceding and current row) as first_known_country,
        row_number() over w_running as seq_no,
        row_number() over (partition by browser, customer_id order by started_at desc, id) as seq_no_2,
        row_number() over w_running_2 as seq_no_3
    from eligible
    window w_running as (partition by browser, country order by started_at desc, id), w_running_2 as (partition by device order by started_at desc, id)
),
enriched as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by device order by seq_no_2 desc, id) as row_seq,
        first_value(country) ignore nulls over (order by seq_no_3, id rows between unbounded preceding and current row) as first_known_country_2,
        round(seq_no / sum(seq_no) over (partition by device), 4) as seq_no_share
    from scored
),
wide as (
    -- Native SQL PIVOT
    select *
    from (
        select
            customer_id,
            device,
            seq_no_share
        from enriched
    ) src
    pivot (
        avg(seq_no_share) as total
        for device in ('mobile', 'tablet')
    )
)
select
    customer_id,
    mobile,
    tablet
from wide
order by mobile desc nulls first
