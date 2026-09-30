with scored as (
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
        ntile(4) over (partition by status order by discount, id) discount_bucket,
        corr(shipping_fee, amount) over (partition by customer_id) corr_shipping_fee_amount,
        corr(amount, discount) over (partition by store_id, status) corr_amount_discount,
        row_number() over w_grp event_seq
    from orders
    window w_grp as (partition by channel, store_id order by discount desc, id)
)
select
    store_id,
    count(case when status = 'RETURNED' then 1 end) as returned_count,
    count(case when status = 'RETURNED' then 1 end) as returned_count_2
from scored
group by store_id
order by returned_count nulls last;
