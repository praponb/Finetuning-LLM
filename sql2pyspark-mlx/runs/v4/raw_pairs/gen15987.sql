with with_windows as (
    -- Running and moving aggregates
    select
        *,
        first_value(quantity, true) over (partition by store_id order by quantity, id rows between unbounded preceding and current row) as first_known_quantity,
        count(quantity) over (partition by product_id) as product_id_count_quantity,
        avg(quantity) over w_part as quantity_avg_5
    from inventory
    window w_part as (partition by product_id order by last_restock, id rows between 5 preceding and current row)
)
-- Multi-level aggregation
select
    store_id,
    count(*) as num_rows,
    count(*) as row_count,
    percentile_approx(quantity, 0.9) as quantity_p90,
    bit_or(quantity) as quantity_bit_or,
    bit_or(quantity) as quantity_bit_or_2
from with_windows
group by store_id
order by quantity_bit_or_2 desc
