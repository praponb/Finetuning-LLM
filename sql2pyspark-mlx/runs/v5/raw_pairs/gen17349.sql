with scored as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        lead(cost) over w_recent as next_cost,
        lag(cost) over w_recent as prev_cost,
        row_number() over w_recent as row_seq
    from shipments
    window w_recent as (partition by carrier, status order by cost, id)
),
enriched as (
    -- Attach orders attributes
    select a.*, d.status
    from scored a
    left join orders d
        on a.order_id = d.id
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from enriched
    qualify row_number() over (partition by status order by prev_cost desc nulls last, id) = 1
),
with_dims as (
    -- Attach orders attributes
    select
        src.*,
        p.status as order_status,
        p.channel,
        p.discount
    from latest src
    left join orders p
        on src.order_id <=> p.id
)
-- Join a dimension and aggregate
select
    o.channel as order_channel,
    o.status as order_status,
    count(*) as cnt,
    max(s.cost) as max_cost,
    sum(s.next_cost) as total_next_cost
from with_dims s
left join orders o on s.order_id = o.id
group by rollup (o.channel, o.status)
order by order_status
