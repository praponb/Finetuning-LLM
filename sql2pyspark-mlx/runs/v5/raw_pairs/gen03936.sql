with valid as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        quantity,
        last_restock,
        coalesce(quantity, 0) as quantity_filled
    from inventory
    where last_restock > to_date('2023-07-15')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when quantity_filled >= 71 then 'High' when quantity_filled >= 23 then 'Medium' else 'Low' end as quantity_filled_level,
        case when quantity >= 267 then 'High' when quantity >= 119 then 'Medium' else 'Low' end as quantity_band
    from valid
    where last_restock >= date '2023-04-05'
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        quantity_band,
        row_number() over (partition by store_id order by last_restock desc, id) as row_seq,
        ntile(10) over (partition by store_id order by quantity, id) as quantity_bucket,
        avg(quantity) over (partition by store_id) as store_id_avg_quantity,
        avg(quantity_filled) over (partition by store_id order by quantity_filled, id rows between 3 preceding and 1 preceding) as quantity_filled_avg_3_prior,
        row_number() over (partition by store_id order by last_restock desc, id) as seq_no
    from cleaned
    where quantity <= 198
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from metrics
    qualify row_number() over (partition by store_id order by seq_no desc nulls last, id) = 1
)
-- Join a dimension and aggregate
select
    s.region,
    count(*) as rows_total,
    min(e.quantity_bucket) as min_quantity_bucket
from deduped e
inner join stores s on e.store_id = s.id
group by s.region
order by region
