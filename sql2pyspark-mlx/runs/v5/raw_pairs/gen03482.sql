with base as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        quantity,
        unit_price,
        coalesce(quantity, 0) as quantity_filled
    from order_items
    where product_id is not null
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select id, quantity
    from base
    qualify row_number() over (partition by product_id order by quantity desc nulls last, id) = 1
),
metrics as (
    -- Ranking and gap features
    select
        id,
        quantity,
        lead(quantity) over (order by quantity, id) as next_quantity,
        row_number() over (order by quantity, id) as row_seq
    from ranked
),
kept_keys as (
    -- Set intersection on keys
    select id
    from metrics
    intersect all
    select id
    from order_items
    where product_id in (36, 37)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join kept_keys k on s.id = k.id
)
-- Aggregate with conditional metrics
select
    case when quantity >= 7 then 'PREMIUM' else 'STANDARD' end as quantity_tier,
    case when next_quantity >= 8 then 'HIGH' else 'LOW' end as next_quantity_tier,
    case when row_seq >= 8 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
    count(*) as cnt,
    bit_and(quantity) as quantity_bit_and,
    round(avg(next_quantity), 2) as avg_next_quantity,
    percentile_approx(row_seq, 0.5) as row_seq_p50,
    approx_percentile(quantity, 0.9) as quantity_p90,
    round(avg(next_quantity), 2) as avg_next_quantity_2
from reconciled
group by quantity_tier, next_quantity_tier, row_seq_tier
order by quantity_p90 nulls last;
