with valid as (
    select
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        year(shipped_date) as shipped_date_year
    from shipments
    where shipped_date >= to_date('2023-09-16')
),
with_windows as (
    select
        id,
        carrier,
        shipped_date,
        cost,
        lag(cost) over (partition by carrier order by shipped_date_year, id) as prev_cost,
        lead(cost) over (partition by carrier order by shipped_date_year, id) as next_cost,
        lead(shipped_date_year) over (partition by carrier order by shipped_date_year, id) as next_shipped_date_year,
        row_number() over (partition by carrier order by shipped_date_year, id) as row_seq,
        sum(shipped_date_year) over (partition by carrier order by shipped_date, id rows between unbounded preceding and current row) as cumulative_shipped_date_year
    from valid
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by carrier order by row_seq desc nulls last, id) as latest_rank
        from with_windows
    ) t
    where latest_rank = 1
),
enriched as (
    select
        *,
        avg(next_cost) over (partition by carrier order by shipped_date, id rows between 6 preceding and 1 preceding) as next_cost_avg_6_prior,
        row_number() over w_recent as row_seq_2,
        dense_rank() over (partition by carrier order by prev_cost nulls last) as prev_cost_dense_rank,
        cume_dist() over w_grp as next_shipped_date_year_cume_dist
    from ranked
    where next_shipped_date_year >= 31
    window w_recent as (order by cost desc, id), w_grp as (partition by carrier order by next_shipped_date_year desc nulls first)
),
valid_shipments as (
    select
        id,
        carrier,
        shipped_date,
        cost,
        prev_cost,
        next_cost,
        next_shipped_date_year,
        row_seq,
        latest_rank,
        next_cost_avg_6_prior,
        row_seq_2,
        next_shipped_date_year_cume_dist,
        coalesce(row_seq_2, 0) as row_seq_2_filled,
        round(row_seq * 0.9, 2) as adjusted_row_seq,
        coalesce(carrier, 'UNKNOWN') as resolved_carrier
    from enriched
    where shipped_date > date '2023-05-23'
),
enriched_shipments as (
    select
        *,
        ntile(3) over (order by next_shipped_date_year_cume_dist, id) as next_shipped_date_year_cume_dist_bucket,
        cume_dist() over (order by next_cost_avg_6_prior desc nulls last) as next_cost_avg_6_prior_cume_dist,
        lead(next_cost, 1, 0) over (order by shipped_date, id) as next_next_cost
    from valid_shipments
),
valid_shipments_2 as (
    select
        *,
        round(cost, 1) as cost_rounded,
        coalesce(carrier, 'OTHER') as carrier_clean
    from enriched_shipments
    where shipped_date >= date '2023-07-08'
      and row_seq_2_filled <= 48
)
select
    id,
    shipped_date,
    cost,
    next_shipped_date_year,
    row_seq,
    next_cost_avg_6_prior,
    next_shipped_date_year_cume_dist,
    resolved_carrier,
    next_shipped_date_year_cume_dist_bucket,
    cost_rounded,
    row_number() over (partition by carrier order by prev_cost, id) as row_seq_3,
    sum(next_next_cost) over (order by row_seq_2, id rows between unbounded preceding and current row) as running_next_next_cost
from valid_shipments_2;
