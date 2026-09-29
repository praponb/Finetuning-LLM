with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        dayofweek(delivered_date) as delivered_date_dayofweek,
        substring(carrier, 1, 3) as carrier_prefix,
        coalesce(carrier, 'OTHER') as resolved_carrier
    from shipments
    where shipped_date > date '2023-03-09'
      and order_id is not null
),
scored as (
    -- Ranking and gap features
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        delivered_date_dayofweek,
        carrier_prefix,
        resolved_carrier,
        first_value(delivered_date_dayofweek) ignore nulls over w_ordered as first_known_delivered_date_dayofweek,
        min(delivered_date_dayofweek) over (partition by order_id) as order_id_min_delivered_date_dayofweek
    from filtered
    window w_ordered as (order by delivered_date_dayofweek, id rows between unbounded preceding and current row)
),
filtered_shipments as (
    -- Keep recent, valid rows
    select
        *,
        date_format(shipped_date, 'yyyy-MM') as shipped_date_label,
        coalesce(carrier, 'N/A') as carrier_filled
    from scored
    where shipped_date > date '2023-08-01'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        carrier,
        delivered_date,
        cost,
        status,
        delivered_date_dayofweek,
        carrier_prefix,
        resolved_carrier,
        first_known_delivered_date_dayofweek,
        shipped_date_label,
        carrier_filled,
        delivered_date_dayofweek * cost as delivered_date_dayofweek_times_cost
    from filtered_shipments
    where cost < 48
),
scored_shipments as (
    -- Window metrics over several different partitions
    select
        id,
        carrier,
        delivered_date,
        cost,
        status,
        delivered_date_dayofweek,
        carrier_prefix,
        resolved_carrier,
        first_known_delivered_date_dayofweek,
        shipped_date_label,
        carrier_filled,
        delivered_date_dayofweek_times_cost,
        row_number() over (partition by status order by delivered_date desc, id) as row_seq,
        ntile(5) over (partition by carrier order by delivered_date_dayofweek_times_cost desc, id) as delivered_date_dayofweek_times_cost_bucket,
        row_number() over w_running as event_seq,
        lag(first_known_delivered_date_dayofweek, 1) over (partition by status order by delivered_date_dayofweek, id) as prev_first_known_delivered_date_dayofweek
    from cleaned
    window w_running as (partition by carrier, status order by delivered_date_dayofweek, id)
),
remaining_keys as (
    -- Set difference on keys
    select carrier
    from scored_shipments
    except
    select carrier
    from shipments
    where shipped_date <= '2023-10-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored_shipments s
    inner join remaining_keys k on s.carrier = k.carrier
),
enriched as (
    -- Ranking and gap features
    select
        id,
        carrier,
        delivered_date,
        cost,
        status,
        delivered_date_dayofweek,
        carrier_prefix,
        resolved_carrier,
        first_known_delivered_date_dayofweek,
        shipped_date_label,
        carrier_filled,
        delivered_date_dayofweek_times_cost,
        row_seq,
        delivered_date_dayofweek_times_cost_bucket,
        event_seq,
        prev_first_known_delivered_date_dayofweek,
        round(row_seq / sum(row_seq) over (partition by status), 4) as row_seq_share,
        rank() over (order by row_seq nulls last) as row_seq_rank,
        rank() over (partition by status, carrier order by row_seq desc nulls last) as row_seq_rank_2
    from verified
)
select
    id,
    carrier,
    delivered_date,
    cost,
    resolved_carrier,
    shipped_date_label,
    carrier_filled,
    row_seq,
    delivered_date_dayofweek_times_cost_bucket
from enriched
order by cost, carrier
