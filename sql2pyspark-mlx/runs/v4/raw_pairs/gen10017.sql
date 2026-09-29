with base as (
    select
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        trim(channel) as clean_channel,
        discount * amount as discount_times_amount
    from orders
    where order_date > date '2023-08-27'
)
select s.*
from base s
where not exists (
        select 1
        from shipments x
        where x.order_id = s.id
          and x.delivered_date <= s.order_date
    );
