with valid as (
    select
        id,
        carrier,
        shipped_date,
        cost,
        initcap(trim(carrier)) as carrier_title,
        substring(status, 1, 2) as status_prefix,
        coalesce(carrier, 'NONE') as resolved_carrier
    from shipments
    where carrier is not null
      and status = 'DELIVERED'
),
latest as (
    select
        id,
        shipped_date,
        cost,
        carrier_title,
        resolved_carrier
    from (
        select *, row_number() over (partition by carrier order by cost, id) as row_num
        from valid
    ) x
    where row_num = 1
),
scored as (
    select
        id,
        shipped_date,
        cost,
        resolved_carrier,
        sum(cost) over w_grp as running_cost,
        row_number() over w_grp_2 as row_seq,
        cume_dist() over (order by cost desc nulls last) as cost_cume_dist
    from latest
    window w_grp as (order by shipped_date, id rows between unbounded preceding and current row), w_grp_2 as (order by cost desc, id)
),
base as (
    select
        id,
        shipped_date,
        cost,
        resolved_carrier,
        running_cost,
        row_seq,
        cost_cume_dist,
        cast(cost_cume_dist as int) as cost_cume_dist_int,
        date_trunc('MONTH', shipped_date) as shipped_date_period
    from scored
    where shipped_date >= date '2023-08-30'
),
with_windows as (
    select
        id,
        resolved_carrier,
        running_cost,
        shipped_date_period,
        min(row_seq) over w_part as last7_min_row_seq,
        avg(row_seq) over w_part as last7_avg_row_seq,
        sum(row_seq) over w_part as last7_sum_row_seq
    from base
    window w_part as (order by row_seq, id rows between 6 preceding and current row)
),
scored_shipments as (
    select
        id,
        running_cost,
        last7_min_row_seq,
        last7_sum_row_seq,
        rank() over (order by last7_min_row_seq desc) as last7_min_row_seq_rank,
        count(last7_avg_row_seq) over (order by cast(shipped_date_period as bigint) range between 7776000 preceding and current row) as last7_avg_row_seq_count_90d
    from with_windows
),
daily as (
    select
        case when last7_min_row_seq_rank >= 9 then 'LARGE' else 'SMALL' end as last7_min_row_seq_rank_tier,
        case when last7_sum_row_seq >= 34 then 'PREMIUM' else 'STANDARD' end as last7_sum_row_seq_tier,
        percentile_approx(last7_avg_row_seq_count_90d, array(0.25, 0.5, 0.75)) as last7_avg_row_seq_count_90d_quartiles,
        count(distinct id) as distinct_ids,
        count(*) as cnt,
        min(last7_avg_row_seq_count_90d) as min_last7_avg_row_seq_count_90d,
        count(distinct id) as distinct_ids_2,
        count(*) as num_rows
    from scored_shipments
    group by rollup (case when last7_min_row_seq_rank >= 9 then 'LARGE' else 'SMALL' end, case when last7_sum_row_seq >= 34 then 'PREMIUM' else 'STANDARD' end)
)
select
    last7_sum_row_seq_tier,
    last7_avg_row_seq_count_90d_quartiles,
    cnt,
    min_last7_avg_row_seq_count_90d,
    distinct_ids_2,
    num_rows
from daily;
