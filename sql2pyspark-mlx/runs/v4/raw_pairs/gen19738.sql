with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, case when cost >= 86 then 'High' when cost >= 16 then 'Medium' else 'Low' end as cost_band
    from shipments
    where shipped_date >= date '2023-10-17'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by order_id, carrier order by delivered_date nulls first, id) as latest_rank
        from filtered
    ) t
    where latest_rank = 1
),
scored as (
    -- Ranking and gap features
    select
        *,
        last_value(carrier, true) over (partition by order_id order by shipped_date, id rows between unbounded preceding and current row) as last_known_carrier,
        row_number() over w_grp as seq_no,
        first_value(cost) ignore nulls over (partition by status, carrier order by latest_rank, id rows between unbounded preceding and current row) as first_known_cost,
        round(cost / sum(cost) over (partition by status), 4) as cost_share
    from deduped
    window w_grp as (partition by carrier order by shipped_date desc, id)
),
enriched as (
    -- Attach orders attributes
    select
        src.*,
        o.amount,
        o.order_date,
        o.status as order_status
    from scored src
    left join orders o
        on src.order_id = o.id
),
with_dims as (
    -- Attach orders attributes
    select a.*, o.discount
    from enriched a
    left join orders o
        on a.order_id = o.id
),
joined as (
    -- Attach orders attributes
    select
        a.*,
        o.status as order_status_2,
        o.order_date as order_order_date,
        o.discount as order_discount
    from with_dims a
    left join orders o
        on a.order_id = o.id
),
enriched_shipments as (
    -- Attach orders attributes
    select e.*, o.discount as order_discount_2
    from joined e
    left join orders o
        on e.order_id = o.id
)
select
    s.id,
    s.order_id,
    s.carrier,
    s.status,
    s.last_known_carrier,
    s.seq_no,
    s.first_known_cost,
    s.cost_share,
    s.order_status,
    case when order_status rlike '^(RETURNED|SHIPPED)$' then 'PRIORITY' else 'GROUP_B' end as order_status_zone,
    (select avg(o2.unit_price) from order_items o2 where o2.order_id = s.order_id) as avg_order_item_unit_price
from enriched_shipments s
order by order_id desc, first_known_cost
