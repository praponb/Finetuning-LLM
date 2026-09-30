with valid as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        date_sub(shipped_date, 67) as shipped_date_minus_67d,
        last_day(shipped_date) as shipped_date_month_end
    from shipments
    where cost < 70
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        carrier,
        cost,
        count(cost) over (partition by carrier order by delivered_date, id rows between unbounded preceding and current row) as running_count_cost,
        avg(cost) over (partition by carrier order by delivered_date, id rows between unbounded preceding and current row) as running_avg_cost,
        sum(cost) over (partition by carrier order by delivered_date, id rows between unbounded preceding and current row) as running_sum_cost,
        sum(cost) over (partition by carrier order by delivered_date, id rows between unbounded preceding and current row) as running_cost
    from valid
    where order_id is not null
),
matched_keys as (
    -- Set difference on keys
    select id, order_id
    from with_windows
    except
    select id, order_id
    from shipments
    where shipped_date < '2024-08-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join matched_keys k on s.id = k.id and s.order_id = k.order_id
),
joined as (
    -- Attach orders attributes
    select src.*, d.status
    from verified src
    left join orders d
        on src.order_id = d.id
),
with_dims as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.carrier,
        s.cost,
        s.running_count_cost,
        s.running_sum_cost,
        s.status,
        p.channel,
        p.status as order_status,
        p.order_date
    from joined s
    left join orders p
        on s.order_id = p.id
)
select
    s.id,
    s.order_id,
    s.carrier,
    s.cost,
    s.running_count_cost,
    s.running_sum_cost,
    s.status,
    s.channel,
    s.order_status,
    case when channel rlike '^(mobile|web)$' then 'EXTENDED' when channel rlike '^(store|partner)$' then 'OTHER_ZONE' else 'PRIORITY' end as channel_zone,
    (select max(t2.cost) from shipments t2 where t2.order_id = s.order_id) as max_shipment_cost
from with_dims s
order by channel nulls last;
