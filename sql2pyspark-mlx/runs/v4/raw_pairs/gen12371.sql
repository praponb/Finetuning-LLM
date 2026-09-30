with filtered as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws('-', device, country) as device_country_key,
        case when country in ('TH', 'SG') then 'Core' else 'Other' end as country_group
    from sessions
    where started_at >= date '2023-04-25'
),
scored as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        device_country_key,
        country_group,
        first_value(country) ignore nulls over (order by started_at, id rows between unbounded preceding and current row) as first_known_country,
        row_number() over (partition by customer_id order by started_at desc, id) as seq_no,
        row_number() over (partition by customer_id, country order by started_at, id) as event_seq
    from filtered
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        device_country_key,
        country_group,
        first_known_country,
        seq_no,
        event_seq,
        row_number() over (partition by browser order by seq_no desc, id) as row_seq,
        lead(seq_no, 1) over (partition by country, browser order by seq_no, id) as next_seq_no,
        lead(seq_no, 1) over (partition by customer_id, country order by seq_no, id) as next_seq_no_2
    from scored
)
select
    s.id,
    s.customer_id,
    s.started_at,
    s.device_country_key,
    s.country_group,
    s.first_known_country,
    s.seq_no,
    s.event_seq,
    s.row_seq,
    (select avg(t2.discount) from orders t2 where t2.customer_id = s.customer_id) as avg_order_discount
from metrics s
order by row_seq nulls first
