with with_dims as (
    -- Attach stores attributes
    select src.*, d.store_name
    from orders src
    join stores d
        on src.store_id = d.id
)
-- Multi-level aggregation
select
    customer_id,
    count(*) filter (where status = 'RETURNED') as returned_rows,
    round(avg(discount), 2) as avg_discount,
    round(avg(shipping_fee), 2) as avg_shipping_fee,
    count(*) as num_rows,
    sort_array(collect_set(status)) as status_list,
    stddev_samp(amount) as amount_stddev
from with_dims
group by customer_id;
