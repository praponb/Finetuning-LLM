with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, length(status) as status_length
    from orders
    where order_date > to_date('2023-06-26')
),
metrics as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        status_length,
        corr(discount, amount) over (partition by status) as corr_discount_amount,
        avg(amount) over (partition by channel order by amount, id rows between 2 preceding and current row) as amount_avg_2,
        row_number() over (order by status_length desc, id) as seq_no,
        avg(amount) over (order by amount, id rows between 2 preceding and current row) as amount_avg_2_2
    from cleaned
),
kept_keys as (
    -- Set intersection on keys
    select customer_id
    from metrics
    intersect
    select customer_id
    from orders
    where order_date between '2023-03-01' and '2023-06-23'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join kept_keys k on s.customer_id = k.customer_id
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        corr(discount, shipping_fee) over (partition by store_id) as corr_discount_shipping_fee,
        cume_dist() over (partition by channel order by shipping_fee desc) as shipping_fee_cume_dist,
        round(seq_no / sum(seq_no) over (partition by store_id), 4) as seq_no_share
    from kept_rows
),
metrics_orders as (
    -- Ranking and gap features
    select
        *,
        max(shipping_fee_cume_dist) over (partition by channel) as channel_max_shipping_fee_cume_dist,
        cume_dist() over (partition by store_id order by seq_no_share) as seq_no_share_cume_dist,
        min(seq_no_share) over (partition by channel) as channel_min_seq_no_share,
        dense_rank() over (order by seq_no_share desc nulls first) as seq_no_share_dense_rank,
        avg(amount_avg_2_2) over (partition by channel) as channel_avg_amount_avg_2_2
    from enriched
)
-- Running and moving aggregates
select
    *,
    cume_dist() over (partition by customer_id, channel order by seq_no_share desc) as seq_no_share_cume_dist_2,
    dense_rank() over (order by amount_avg_2_2 nulls first) as amount_avg_2_2_dense_rank,
    cume_dist() over (order by discount desc nulls first) as discount_cume_dist
from metrics_orders
