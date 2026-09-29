with with_windows as (
    -- Ranking and gap features
    select
        *,
        count(discount) over w_ordered last3_count_discount,
        avg(discount) over w_ordered last3_avg_discount,
        min(discount) over w_ordered last3_min_discount
    from orders
    where year(order_date) = 2023
    window w_ordered as (partition by customer_id order by order_date asc, id asc rows between 2 preceding and current row)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, cast(last3_avg_discount as int) last3_avg_discount_int
    from with_windows
    where last3_avg_discount between 0.24 and 0.4
),
scored as (
    -- Ranking and gap features
    select
        *,
        avg(last3_avg_discount_int) over (partition by customer_id order by last3_avg_discount asc, id asc rows between 6 preceding and 1 preceding) last3_avg_discount_int_avg_6_prior,
        max(discount) over (partition by customer_id) customer_id_max_discount,
        ntile(3) over (order by last3_avg_discount_int desc, id asc) last3_avg_discount_int_bucket,
        ntile(10) over (partition by store_id, status order by last3_avg_discount_int asc, id asc) last3_avg_discount_int_bucket_2
    from cleaned
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_id,
        s.status,
        s.channel,
        s.discount,
        s.last3_count_discount,
        s.last3_avg_discount_int_avg_6_prior,
        s.last3_avg_discount_int_bucket,
        latest.order_item_quantity
    from scored s
    left join lateral (
            select c.quantity as order_item_quantity
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(order_item_quantity) over w_part order_item_quantity_avg_5,
        sum(order_item_quantity) over (partition by channel, status order by last3_count_discount asc, id asc rows between unbounded preceding and current row) running_order_item_quantity,
        avg(order_item_quantity) over w_part_2 order_item_quantity_avg_5_prior
    from latest_match
    window w_part as (order by discount asc, id asc rows between 5 preceding and current row), w_part_2 as (partition by channel order by last3_avg_discount_int_bucket asc, id asc rows between 5 preceding and 1 preceding)
)
select
    id,
    status,
    last3_count_discount,
    last3_avg_discount_int_avg_6_prior,
    last3_avg_discount_int_bucket,
    order_item_quantity_avg_5_prior
from metrics
order by last3_avg_discount_int_avg_6_prior desc, id asc;
