with base as (
    -- Basic cleansing and defaults
    select
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cast(cost as int) as cost_int,
        length(carrier) as carrier_length,
        coalesce(carrier, 'OTHER') as resolved_carrier
    from shipments
    where carrier is not null
      and delivered_date > date '2023-04-25'
),
base_shipments as (
    -- Basic cleansing and defaults
    select
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_int,
        carrier_length,
        resolved_carrier,
        add_months(delivered_date, 1) as delivered_date_plus_months,
        last_day(shipped_date) as shipped_date_month_end
    from base
    where carrier is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        shipped_date,
        cost,
        carrier_length,
        delivered_date_plus_months
    from base_shipments
    qualify row_number() over (partition by status order by cost desc nulls first, id) = 1
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        shipped_date,
        cost,
        carrier_length,
        delivered_date_plus_months,
        round(cost * 1.07, 2) as cost_scaled
    from deduped
    where shipped_date > date '2023-04-15'
),
scored as (
    -- Ranking and gap features
    select
        *,
        last_value(cost) ignore nulls over (order by shipped_date, id rows between unbounded preceding and current row) as last_known_cost,
        row_number() over w_grp as seq_no
    from valid
    window w_grp as (order by shipped_date, id)
)
select
    id,
    shipped_date,
    cost,
    last_known_cost,
    seq_no
from scored
order by last_known_cost nulls first
