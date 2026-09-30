with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        add_months(shipped_date, 12) as shipped_date_plus_months
    from shipments
    where delivered_date > date '2023-04-19'
      and shipped_date > '2023-08-01'
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by carrier order by shipped_date desc, id) as row_seq,
        row_number() over (partition by carrier order by shipped_date_plus_months, id) as event_seq,
        row_number() over (partition by carrier, order_id order by delivered_date, id) as seq_no
    from cleaned
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        order_id,
        carrier,
        shipped_date,
        shipped_date_plus_months,
        event_seq,
        seq_no
    from with_windows
    qualify row_number() over (partition by carrier order by delivered_date, id) = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        seq_no,
        lead(event_seq, 2) over (partition by carrier, order_id order by event_seq, id) as next_event_seq,
        avg(seq_no) over (order by event_seq, id rows between 5 preceding and 1 preceding) as seq_no_avg_5_prior
    from latest
    where seq_no is not null
),
daily as (
    -- Aggregate with conditional metrics
    select
        case when seq_no >= 8 then 'PREMIUM' else 'STANDARD' end as seq_no_tier,
        seq_no,
        count(distinct id) as distinct_ids,
        min(seq_no_avg_5_prior) as min_seq_no_avg_5_prior,
        count(*) as cnt,
        percentile_approx(seq_no, 0.95) as seq_no_p95,
        count(distinct id) as distinct_ids_2,
        count(distinct id) as distinct_ids_3
    from scored
    group by case when seq_no >= 8 then 'PREMIUM' else 'STANDARD' end, seq_no
)
select
    seq_no_tier,
    seq_no,
    distinct_ids,
    min_seq_no_avg_5_prior,
    distinct_ids_3
from daily
