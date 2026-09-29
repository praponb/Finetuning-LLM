with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        dayofweek(shipped_date) as shipped_date_dayofweek,
        year(shipped_date) as shipped_date_year,
        coalesce(carrier, 'N/A') as resolved_carrier
    from shipments
    where order_id is not null
      and status = 'DELIVERED'
),
enriched as (
    -- Attach orders attributes
    select a.*, p.channel
    from base a
    left join orders p
        on a.order_id = p.id
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        delivered_date,
        cost,
        status,
        shipped_date_dayofweek,
        shipped_date_year,
        resolved_carrier,
        row_number() over (order by shipped_date, id) as row_seq,
        avg(shipped_date_year) over (partition by carrier order by cost, id rows between 2 preceding and current row) as shipped_date_year_avg_2,
        row_number() over (partition by carrier, order_id order by shipped_date desc, id) as event_seq
    from enriched
    where shipped_date > '2024-10-15'
),
matched_keys as (
    -- Set difference on keys
    select id, status
    from metrics
    except
    select id, status
    from shipments
    where carrier not in ('Flash', 'UPS')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join matched_keys k on s.id = k.id and s.status = k.status
)
select
    id,
    delivered_date,
    cost,
    shipped_date_dayofweek,
    shipped_date_year,
    resolved_carrier,
    shipped_date_year_avg_2,
    event_seq
from kept_rows
order by shipped_date_dayofweek desc, delivered_date
