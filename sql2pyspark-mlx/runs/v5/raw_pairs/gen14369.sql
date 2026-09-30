with base as (
    select *, concat_ws('/', channel, status) channel_status_key
    from orders
    where order_date > date '2023-08-23'
      and customer_id is not null
),
with_last as (
    select
        s.id,
        s.customer_id,
        s.amount,
        x.order_item_unit_price,
        x.order_item_quantity
    from base s
    join lateral (
            select c.unit_price as order_item_unit_price, c.quantity as order_item_quantity
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
scored as (
    select
        *,
        min(amount) over w_part last7_min_amount,
        sum(amount) over w_part last7_sum_amount,
        count(amount) over w_part last7_count_amount
    from with_last
    window w_part as (partition by customer_id order by amount, id rows between 6 preceding and current row)
),
matched_keys as (
    select id, customer_id
    from scored
    except all
    select id, customer_id
    from orders
    where channel in ('web', 'mobile')
),
reconciled as (
    select s.*
    from scored s
    inner join matched_keys k on s.id = k.id and s.customer_id = k.customer_id
)
select
    customer_id,
    amount,
    order_item_quantity,
    last7_count_amount
from reconciled
