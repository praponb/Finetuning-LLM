with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        cast(shipping_fee as int) as shipping_fee_int
    from orders
    where order_date >= date '2023-02-23'
      and channel is distinct from 'mobile'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from order_items ch
            where ch.order_id = s.id
              and ch.unit_price <= 310
        )
),
latest as (
    -- QUALIFY-style window filter
    select *
    from not_flagged
    qualify rank() over (partition by customer_id order by order_date) = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        row_number() over (order by discount desc, id) as event_seq,
        avg(amount) over (order by shipping_fee_int, id rows between 6 preceding and current row) as amount_avg_6,
        ntile(5) over (order by amount desc, id) as amount_bucket,
        row_number() over w_ordered as seq_no,
        sum(discount) over w_grp as store_id_sum_discount
    from latest
    window w_ordered as (partition by store_id order by shipping_fee_int, id), w_grp as (partition by store_id)
),
ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        customer_id,
        order_date,
        channel,
        amount,
        discount,
        amount_bucket
    from (
        select *, row_number() over (partition by channel order by order_date, id) as dedup_rank
        from metrics
    ) t
    where dedup_rank = 1
),
with_dims as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.amount,
        a.discount,
        a.amount_bucket,
        b.age,
        b.email,
        b.credit_limit
    from ranked a
    left join customers b
        on a.customer_id = b.id
),
not_flagged_orders as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_dims s
    where not exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.order_id = 5
        )
)
-- Join a dimension and aggregate
select
    dim.segment,
    count(*) as rows_total,
    max(src.amount_bucket) as max_amount_bucket,
    min(src.amount) as min_amount
from not_flagged_orders src
join customers dim on src.customer_id = dim.id
group by dim.segment
order by rows_total nulls last
