with with_windows as (
    -- Running and moving aggregates
    select
        *,
        rank() over (partition by order_id order by quantity desc nulls first) as quantity_rank,
        avg(quantity) over w_hist as quantity_avg_5,
        round(unit_price / sum(unit_price) over (partition by product_id, order_id), 4) as unit_price_share
    from order_items
    where quantity between 4 and 6
    window w_hist as (partition by product_id, order_id order by quantity, id rows between 5 preceding and current row)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        unit_price,
        quantity_rank,
        sum(quantity_rank) over w_recent as product_id_sum_quantity_rank,
        sum(unit_price) over (partition by product_id order by unit_price_share, id rows between unbounded preceding and current row) as running_unit_price,
        percent_rank() over (partition by order_id, product_id order by quantity desc) as quantity_percent_rank,
        corr(unit_price, quantity) over w_recent as corr_unit_price_quantity
    from with_windows
    window w_recent as (partition by product_id)
),
with_dims as (
    -- Attach orders attributes
    select e.*, o.status
    from enriched e
    inner join orders o
        on e.order_id = o.id
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by status order by unit_price nulls first, id) as row_num
        from with_dims
    ) ranked
    where row_num = 1
),
with_dims_order_items as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.unit_price,
        a.quantity_rank,
        a.product_id_sum_quantity_rank,
        a.running_unit_price,
        d.shipping_fee,
        d.status as order_status
    from ranked a
    left join orders d
        on a.order_id = d.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        case when shipping_fee >= 37 then 'High' when shipping_fee >= 17 then 'Medium' else 'Low' end as shipping_fee_band,
        coalesce(order_status, 'DIRECT') as resolved_order_status
    from with_dims_order_items
    where order_status is not null
),
kept_keys as (
    -- Set intersection on keys
    select order_id, id
    from filtered
    intersect
    select order_id, id
    from order_items
    where unit_price >= 200
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join kept_keys k on s.order_id = k.order_id and s.id = k.id
)
select
    s.order_id,
    s.running_unit_price,
    s.shipping_fee,
    s.order_status,
    s.shipping_fee_band,
    case when order_status rlike '^(RETURNED|CANCELLED)$' then 'EXTENDED' when order_status rlike '^(COMPLETED|PENDING)$' then 'GROUP_B' else 'OTHER_ZONE' end as order_status_zone,
    (select min(x.amount) from payments x where x.order_id = s.order_id) as min_payment_amount
from verified s;
