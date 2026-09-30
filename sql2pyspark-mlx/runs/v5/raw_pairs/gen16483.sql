with deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        carrier,
        delivered_date,
        cost
    from shipments
    qualify row_number() over (partition by carrier, order_id order by shipped_date desc nulls first, id) = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by carrier order by delivered_date, id) as seq_no,
        avg(cost) over (order by cost, id rows between 2 preceding and 1 preceding) as cost_avg_2_prior,
        dense_rank() over (order by cost nulls first) as cost_dense_rank
    from deduped
    where cost >= 42
),
deduped_shipments as (
    -- Deduplicate with ROW_NUMBER
    select *
    from scored
    qualify row_number() over (partition by carrier order by cost_avg_2_prior nulls last, id) = 1
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        delivered_date,
        cost,
        seq_no,
        quarter(delivered_date) as delivered_date_quarter
    from deduped_shipments
    where carrier is distinct from 'UPS'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        cost,
        seq_no,
        delivered_date_quarter,
        ntile(3) over (order by cost desc, id) as cost_bucket,
        lead(seq_no, 1) over (order by seq_no, id) as next_seq_no,
        row_number() over w_recent as seq_no_2
    from base
    window w_recent as (order by delivered_date, id)
),
base_shipments as (
    -- Keep recent, valid rows
    select
        id,
        cost,
        seq_no,
        delivered_date_quarter,
        next_seq_no,
        seq_no_2,
        coalesce(delivered_date_quarter, 0) as delivered_date_quarter_filled
    from with_windows
    where cost_bucket >= 5
),
scored_shipments as (
    -- Running and moving aggregates
    select
        id,
        cost,
        delivered_date_quarter,
        next_seq_no,
        seq_no_2,
        delivered_date_quarter_filled,
        row_number() over (order by next_seq_no, id) as row_seq,
        avg(delivered_date_quarter_filled) over (order by seq_no, id rows between 2 preceding and current row) as delivered_date_quarter_filled_avg_2,
        rank() over w_hist as seq_no_rank,
        avg(cost) over (order by cost, id rows between 6 preceding and current row) as cost_avg_6,
        rank() over w_grp as delivered_date_quarter_rank
    from base_shipments
    window w_hist as (order by seq_no desc nulls last), w_grp as (order by delivered_date_quarter)
)
-- Multi-level aggregation
select
    case when cost >= 90 then 'PREMIUM' else 'STANDARD' end as cost_tier,
    min(next_seq_no) as min_next_seq_no,
    round(avg(delivered_date_quarter_filled), 2) as avg_delivered_date_quarter_filled,
    avg(delivered_date_quarter_rank) as avg_delivered_date_quarter_rank
from scored_shipments
group by case when cost >= 90 then 'PREMIUM' else 'STANDARD' end
