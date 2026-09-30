with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        hour(txn_time) txn_time_hour
    from transactions
    where txn_time > to_date('2023-04-07')
),
enriched as (
    -- Attach accounts attributes
    select
        src.id,
        src.account_id,
        src.txn_type,
        src.amount,
        src.merchant,
        src.txn_time_hour,
        d.opened_date,
        d.account_type,
        d.balance
    from filtered src
    left join accounts d
        on src.account_id = d.id
    where d.customer_id in (19, 23, 25, 38)
),
latest as (
    -- QUALIFY-style window filter
    select *
    from enriched
    qualify rank() over (partition by merchant order by amount desc nulls first) = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        account_id,
        txn_type,
        txn_time_hour,
        opened_date,
        balance,
        sum(amount) over (partition by account_type, merchant order by opened_date, id rows between unbounded preceding and current row) running_sum_amount,
        count(amount) over (partition by account_type, merchant order by opened_date, id rows between unbounded preceding and current row) running_count_amount,
        min(amount) over (partition by account_type, merchant order by opened_date, id rows between unbounded preceding and current row) running_min_amount
    from latest
),
scored as (
    -- Running and moving aggregates
    select
        id,
        account_id,
        txn_type,
        opened_date,
        balance,
        running_sum_amount,
        running_count_amount,
        running_min_amount,
        lead(balance) over w_running next_balance,
        lag(balance) over w_running prev_balance,
        lead(txn_time_hour) over w_running next_txn_time_hour,
        row_number() over w_running event_seq
    from metrics
    window w_running as (partition by account_id, txn_type order by balance, id)
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by txn_type order by opened_date desc, id) as rank_in_group
        from scored
    ) x
    where rank_in_group = 1
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        avg(running_count_amount) over (order by running_count_amount, id rows between 2 preceding and 1 preceding) running_count_amount_avg_2_prior,
        sum(prev_balance) over (partition by txn_type order by rank_in_group, id rows between unbounded preceding and current row) running_prev_balance,
        ntile(3) over (order by event_seq desc, id) event_seq_bucket
    from deduped
)
-- Aggregate with conditional metrics
select
    date_trunc('year', opened_date) as year_start,
    case when event_seq >= 7 then 'PREMIUM' else 'STANDARD' end as event_seq_tier,
    round(sum(event_seq_bucket) / nullif(count(case when txn_type = 'REFUND' then 1 end), 0), 2) as event_seq_bucket_per_refund,
    approx_percentile(next_txn_time_hour, array(0.25, 0.5, 0.75)) as next_txn_time_hour_quartiles,
    count_if(txn_type = 'REFUND') as refund_events,
    percent_rank() over (partition by date_trunc('year', opened_date) order by round(sum(event_seq_bucket) / nullif(count(case when txn_type = 'REFUND' then 1 end), 0), 2) desc) as event_seq_bucket_per_refund_percent_rank
from with_windows
group by date_trunc('year', opened_date), case when event_seq >= 7 then 'PREMIUM' else 'STANDARD' end
having round(sum(event_seq_bucket) / nullif(count(case when txn_type = 'REFUND' then 1 end), 0), 2) > 0;
