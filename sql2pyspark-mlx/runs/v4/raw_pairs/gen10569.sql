with scored as (
    -- Window metrics over several different partitions
    select
        *,
        sum(units) over (partition by product_id order by revenue, id rows between unbounded preceding and current row) as running_units,
        row_number() over (partition by product_id order by sale_date desc, id) as event_seq
    from store_sales
)
-- Aggregate with conditional metrics
select
    product_id,
    min(running_units) as min_running_units,
    round(avg(event_seq), 2) as avg_event_seq,
    bit_or(units) as units_bit_or
from scored
group by product_id
