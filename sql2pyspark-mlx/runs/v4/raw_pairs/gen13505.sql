with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        shipped_date,
        cost,
        status,
        upper(carrier) as carrier_upper
    from shipments
    where delivered_date > to_date('2024-01-10')
      and delivered_date > '2024-04-01'
),
kept_keys as (
    -- Set difference on keys
    select status, id
    from cleaned
    except distinct
    select status, id
    from shipments
    where carrier = 'FedEx'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.status = k.status and s.id = k.id
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        date_add(shipped_date, 41) as shipped_date_plus_41d,
        last_day(shipped_date) as shipped_date_month_end
    from verified
    where status in ('RETURNED', 'LOST')
),
grouped as (
    -- Multi-level aggregation
    select
        case when cost >= 97 then 'HIGH' else 'LOW' end as cost_tier,
        count(*) as event_count,
        stddev_samp(cost) as cost_stddev,
        approx_percentile(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
        percentile_approx(cost, array(0.25, 0.5, 0.75)) as cost_quartiles_2,
        stddev_samp(cost) as cost_stddev_2,
        sum(case when status = 'LOST' then cost else 0 end) as lost_cost
    from filtered
    group by case when cost >= 97 then 'HIGH' else 'LOW' end
)
select
    cost_tier,
    cost_stddev,
    cost_quartiles,
    cost_quartiles_2,
    cost_stddev_2,
    lost_cost
from grouped
order by cost_tier desc nulls last
limit 20
