with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        dayofweek(delivered_date) as delivered_date_dayofweek,
        coalesce(carrier, 'DIRECT') as carrier_filled
    from shipments
    where shipped_date > date '2023-05-22'
      and carrier is not null
),
cleaned_shipments as (
    -- Keep recent, valid rows
    select
        *,
        datediff(current_date(), shipped_date) as days_since_shipped_date,
        upper(carrier) as carrier_upper,
        coalesce(carrier, 'NONE') as carrier_filled_2
    from cleaned
    where carrier is not null
      and delivered_date > date '2023-03-11'
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        delivered_date_dayofweek,
        carrier_filled,
        days_since_shipped_date,
        carrier_upper,
        carrier_filled_2,
        case when days_since_shipped_date >= 73 then 'High' when days_since_shipped_date >= 30 then 'Medium' else 'Low' end as days_since_shipped_date_band
    from cleaned_shipments
    where shipped_date > date '2023-08-18'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by days_since_shipped_date nulls last) as days_since_shipped_date_dense_rank,
        row_number() over (partition by carrier order by shipped_date, id) as seq_no,
        sum(delivered_date_dayofweek) over (order by shipped_date, id rows between unbounded preceding and current row) as cumulative_delivered_date_dayofweek,
        ntile(10) over (partition by carrier order by days_since_shipped_date, id) as days_since_shipped_date_bucket,
        percent_rank() over (partition by carrier order by days_since_shipped_date desc nulls first) as days_since_shipped_date_percent_rank
    from filtered
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by carrier order by days_since_shipped_date_dense_rank desc, id) as dedup_rank
    from metrics
    qualify dedup_rank = 1
)
-- Multi-level aggregation
select
    case when days_since_shipped_date >= 84 then 'LARGE' else 'SMALL' end as days_since_shipped_date_tier,
    date_trunc('month', delivered_date) as activity_month,
    carrier,
    round(sum(days_since_shipped_date_dense_rank) / nullif(count(case when status = 'RETURNED' then 1 end), 0), 2) as days_since_shipped_date_dense_rank_per_returned,
    size(collect_set(carrier)) as num_carriers,
    count(*) filter (where carrier = 'Flash') as flash_rows
from latest
group by cube (case when days_since_shipped_date >= 84 then 'LARGE' else 'SMALL' end, date_trunc('month', delivered_date), carrier)
having flash_rows > 1
order by activity_month desc;
