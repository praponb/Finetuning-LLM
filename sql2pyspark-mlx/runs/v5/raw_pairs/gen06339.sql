with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        date_sub(last_restock, 60) as last_restock_minus_60d,
        floor(quantity) as quantity_whole
    from inventory
    where last_restock between '2024-02-07' and '2024-09-06'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        quantity,
        quantity_whole,
        ntile(10) over (partition by product_id order by quantity_whole, id) as quantity_whole_bucket,
        row_number() over w_part as row_seq
    from cleaned
    window w_part as (order by last_restock_minus_60d desc, id)
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        quantity,
        quantity_whole,
        quantity_whole_bucket,
        row_seq,
        case when quantity_whole >= 61 then 'High' when quantity_whole >= 18 then 'Medium' else 'Low' end as quantity_whole_tier,
        round(quantity / row_seq, 2) as quantity_per_row_seq
    from enriched
    where quantity_whole_bucket >= 4
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        lead(quantity_per_row_seq, 2) over (order by row_seq, id) as next_quantity_per_row_seq,
        avg(row_seq) over (order by quantity_whole, id rows between 6 preceding and 1 preceding) as row_seq_avg_6_prior,
        rank() over (order by row_seq desc nulls last) as row_seq_rank,
        sum(quantity_whole) over (order by quantity_per_row_seq, id rows between unbounded preceding and current row) as cumulative_quantity_whole
    from filtered
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, cast(quantity_per_row_seq as int) as quantity_per_row_seq_int
    from metrics
    where next_quantity_per_row_seq < 78
),
daily as (
    -- Aggregate with conditional metrics
    select
        case when quantity_whole >= 75 then 'HIGH' else 'LOW' end as quantity_whole_tier,
        case when row_seq >= 8 then 'LARGE' else 'SMALL' end as row_seq_tier,
        avg(quantity_per_row_seq) as avg_quantity_per_row_seq,
        round(avg(quantity_per_row_seq), 2) as avg_quantity_per_row_seq_2
    from valid
    group by case when quantity_whole >= 75 then 'HIGH' else 'LOW' end, case when row_seq >= 8 then 'LARGE' else 'SMALL' end
)
select
    quantity_whole_tier,
    row_seq_tier,
    avg_quantity_per_row_seq,
    avg_quantity_per_row_seq_2
from daily
order by quantity_whole_tier, avg_quantity_per_row_seq_2;
