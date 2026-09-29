with enriched as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ntile(5) over (order by quantity desc, id) as quantity_bucket,
        row_number() over (order by unit_price desc, id) as row_seq,
        first_value(unit_price) ignore nulls over (partition by product_id, order_id order by quantity, id rows between unbounded preceding and current row) as first_known_unit_price
    from order_items
)
-- Multi-level aggregation
select
    case when row_seq >= 9 then 'LARGE' else 'SMALL' end as row_seq_tier,
    order_id,
    sum(quantity) as total_quantity,
    round(avg(quantity_bucket), 2) as avg_quantity_bucket,
    dense_rank() over (partition by case when row_seq >= 9 then 'LARGE' else 'SMALL' end order by round(avg(quantity_bucket), 2) desc) as avg_quantity_bucket_dense_rank
from enriched
group by case when row_seq >= 9 then 'LARGE' else 'SMALL' end, order_id;
