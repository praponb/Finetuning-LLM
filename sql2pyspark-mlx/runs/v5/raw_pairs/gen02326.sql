with with_windows as (
    select
        *,
        max(amount) over w_running running_max_amount,
        count(amount) over w_running running_count_amount,
        sum(amount) over w_running running_sum_amount
    from transactions
    where amount between 1500 and 3900
    window w_running as (partition by account_id, merchant order by txn_time asc, id asc rows between unbounded preceding and current row)
),
gaps as (
    select *, case when lag(txn_time) over (partition by merchant order by txn_time asc, id asc) is null
                 or txn_time > lag(txn_time) over (partition by merchant order by txn_time asc, id asc) + interval 90 days then 1 else 0 end as is_new_session
    from with_windows
),
islands as (
    select *, sum(is_new_session) over (partition by merchant order by txn_time asc, id asc rows between unbounded preceding and current row) as burst_id
    from gaps
),
enriched as (
    select
        *,
        lag(running_sum_amount) over w_running prev_running_sum_amount,
        lag(amount) over w_running prev_amount,
        lead(amount) over w_running next_amount,
        row_number() over w_running row_seq,
        first_value(running_max_amount, true) over (partition by account_id order by running_sum_amount asc, id asc rows between unbounded preceding and current row) first_known_running_max_amount
    from islands
    window w_running as (partition by txn_type order by amount asc, id asc)
),
metrics as (
    select
        *,
        avg(prev_amount) over w_hist last4_avg_prev_amount,
        min(prev_amount) over w_hist last4_min_prev_amount,
        count(prev_amount) over w_hist last4_count_prev_amount,
        avg(running_sum_amount) over (partition by merchant) merchant_avg_running_sum_amount
    from enriched
    window w_hist as (partition by txn_type, account_id order by prev_running_sum_amount asc, id asc rows between 3 preceding and current row)
),
metrics_transactions as (
    select
        *,
        rank() over w_running last4_count_prev_amount_rank,
        round(running_sum_amount / sum(running_sum_amount) over (partition by merchant), 4) running_sum_amount_share,
        cume_dist() over (partition by txn_type, account_id order by burst_id desc) burst_id_cume_dist,
        last_value(prev_amount) ignore nulls over (partition by merchant order by running_max_amount asc, id asc rows between unbounded preceding and current row) last_known_prev_amount
    from metrics
    window w_running as (partition by txn_type, merchant order by last4_count_prev_amount desc nulls first)
)
select
    case when burst_id_cume_dist >= 0.84 then 'PREMIUM' else 'STANDARD' end as burst_id_cume_dist_tier,
    date_trunc('year', txn_time) as year_start,
    account_id,
    (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as span_minutes,
    count(*) as num_rows,
    max(txn_time) as last_seen,
    sum(case when merchant = 'Shell' then last4_count_prev_amount_rank else 0 end) as shell_last4_count_prev_amount_rank,
    count_if(txn_type = 'REFUND') as refund_events,
    round(sum(running_count_amount) / nullif(count(case when merchant = 'Shell' then 1 end), 0), 2) as running_count_amount_per_shell,
    grouping_id() as grouping_id
from metrics_transactions
group by cube (case when burst_id_cume_dist >= 0.84 then 'PREMIUM' else 'STANDARD' end, date_trunc('year', txn_time), account_id)
having (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 < 1000000
order by running_count_amount_per_shell desc nulls last
