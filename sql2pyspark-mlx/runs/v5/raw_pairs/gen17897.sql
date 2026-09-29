with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        quantity,
        unit_price,
        quantity * unit_price as line_total,
        round(quantity / unit_price, 2) as line_total_2
    from order_items
    where unit_price < 180
),
metrics as (
    -- Ranking and gap features
    select
        id,
        quantity,
        line_total_2,
        dense_rank() over (partition by order_id order by unit_price desc nulls last) as unit_price_dense_rank,
        ntile(4) over (partition by order_id order by line_total desc, id) as line_total_bucket
    from filtered
    where quantity <= 3
),
metrics_order_items as (
    -- Running and moving aggregates
    select
        id,
        quantity,
        line_total_bucket,
        avg(line_total_2) over (order by unit_price_dense_rank, id rows between 2 preceding and current row) as last3_avg_line_total_2,
        min(line_total_2) over (order by unit_price_dense_rank, id rows between 2 preceding and current row) as last3_min_line_total_2,
        sum(line_total_2) over (order by unit_price_dense_rank, id rows between 2 preceding and current row) as last3_sum_line_total_2,
        max(line_total_2) over (order by unit_price_dense_rank, id rows between 2 preceding and current row) as last3_max_line_total_2
    from metrics
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (order by last3_sum_line_total_2 nulls last) as last3_sum_line_total_2_dense_rank,
        cume_dist() over (order by last3_avg_line_total_2 desc) as last3_avg_line_total_2_cume_dist,
        last_value(quantity) ignore nulls over w_part as last_known_quantity
    from metrics_order_items
    window w_part as (order by line_total_bucket, id rows between unbounded preceding and current row)
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        quantity,
        line_total_bucket,
        last3_avg_line_total_2,
        last3_min_line_total_2,
        last3_sum_line_total_2,
        last3_max_line_total_2,
        last3_sum_line_total_2_dense_rank,
        last3_avg_line_total_2_cume_dist,
        last_known_quantity,
        case when last_known_quantity >= 9 then 'High' when last_known_quantity >= 4 then 'Medium' else 'Low' end as last_known_quantity_level
    from scored
    where last3_avg_line_total_2 between 24 and 43
)
select
    id,
    quantity,
    line_total_bucket,
    last3_avg_line_total_2,
    last3_min_line_total_2,
    last3_sum_line_total_2,
    last3_sum_line_total_2_dense_rank,
    last_known_quantity
from valid
order by id desc;
