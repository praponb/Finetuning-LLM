with valid as (
    -- Filter the raw rows and derive helper columns
    select *, case when carrier in ('FedEx', 'Kerry') then 'Tier 1' else 'Other' end as carrier_group
    from shipments
    where shipped_date > date '2023-03-26'
),
scored as (
    -- Ranking and gap features
    select
        *,
        first_value(carrier, true) over (partition by status order by shipped_date, id rows between unbounded preceding and current row) as first_known_carrier,
        row_number() over (partition by carrier order by delivered_date desc, id) as row_seq,
        avg(cost) over (partition by order_id, status order by shipped_date, id rows between 6 preceding and 1 preceding) as cost_avg_6_prior
    from valid
    where status <> 'IN_TRANSIT'
),
valid_shipments as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        carrier_group,
        first_known_carrier,
        row_seq,
        last_day(shipped_date) as shipped_date_month_end
    from scored
    where shipped_date > date '2023-04-04'
),
enriched as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.delivered_date,
        src.cost,
        src.carrier_group,
        src.first_known_carrier,
        src.row_seq,
        src.shipped_date_month_end,
        p.order_date,
        p.shipping_fee
    from valid_shipments src
    left join orders p
        on src.order_id <=> p.id
)
-- Window metrics over several different partitions
select
    id,
    delivered_date,
    carrier_group,
    first_known_carrier,
    order_date,
    shipping_fee,
    avg(cost) over (partition by order_id) as order_id_avg_cost,
    ntile(4) over w_part as row_seq_bucket,
    ntile(4) over (partition by order_id order by cost desc, id) as cost_bucket
from enriched
window w_part as (partition by order_id order by row_seq desc, id);
