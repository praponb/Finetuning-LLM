with enriched as (
    -- Ranking and gap features
    select
        *,
        avg(cost) over w_running last4_avg_cost,
        sum(cost) over w_running last4_sum_cost,
        count(cost) over w_running last4_count_cost
    from shipments
    where status in ('IN_TRANSIT', 'DELIVERED')
    window w_running as (partition by status, carrier order by shipped_date asc, id asc rows between 3 preceding and current row)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(cost / sum(cost) over (partition by status, carrier), 4) cost_share,
        sum(last4_count_cost) over (order by last4_sum_cost asc, id asc rows between unbounded preceding and current row) cumulative_last4_count_cost,
        first_value(carrier) ignore nulls over (partition by carrier, status order by delivered_date asc, id asc rows between unbounded preceding and current row) first_known_carrier
    from enriched
),
kept_keys as (
    -- Set intersection on keys
    select carrier, id
    from scored
    intersect
    select carrier, id
    from shipments
    where status = 'IN_TRANSIT'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join kept_keys k on s.carrier = k.carrier and s.id = k.id
),
metrics as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by status order by last4_sum_cost desc nulls last) last4_sum_cost_dense_rank,
        max(cumulative_last4_count_cost) over (partition by carrier) carrier_max_cumulative_last4_count_cost,
        round(cost / sum(cost) over (partition by carrier), 4) cost_share_2,
        avg(cost_share) over (partition by status, order_id) status_avg_cost_share,
        round(last4_avg_cost / sum(last4_avg_cost) over (partition by status, order_id), 4) last4_avg_cost_share
    from kept_rows
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        last4_sum_cost_dense_rank / nullif(cost_share_2, 0) last4_sum_cost_dense_rank_per_cost_share_2,
        floor(cost_share_2) cost_share_2_whole
    from metrics
    where status in ('LOST', 'RETURNED')
)
select
    id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    last4_avg_cost,
    last4_sum_cost,
    cost_share
from valid
order by shipped_date desc nulls first;
