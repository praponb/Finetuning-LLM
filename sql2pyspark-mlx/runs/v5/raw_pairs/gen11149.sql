with scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over w_part as event_seq,
        avg(amount) over (partition by merchant order by amount, id rows between 2 preceding and 1 preceding) as amount_avg_2_prior,
        row_number() over w_recent as row_seq
    from transactions
    where amount > 3300
    window w_part as (order by txn_time, id), w_recent as (order by amount desc, id)
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval '60' day then 1 else 0 end as is_new_burst
    from scored
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as session_id
    from gaps
),
with_dims as (
    -- Attach accounts attributes
    select
        e.*,
        d.balance,
        d.status,
        d.account_type
    from sessionized e
    left join accounts d
        on e.account_id = d.id
),
scored_transactions as (
    -- Ranking and gap features
    select
        *,
        row_number() over (order by event_seq, id) as row_seq_2,
        avg(event_seq) over (partition by status order by cast(txn_time as bigint) range between 7776000 preceding and current row) as event_seq_avg_90d,
        lag(session_id, 2, 0) over (partition by account_type order by txn_time, id) as prev_session_id
    from with_dims
),
metrics as (
    -- Ranking and gap features
    select
        id,
        account_id,
        amount,
        is_flagged,
        row_seq,
        is_new_burst,
        balance,
        status,
        account_type,
        first_value(event_seq) ignore nulls over (partition by merchant, txn_type order by amount_avg_2_prior, id rows between unbounded preceding and current row) as first_known_event_seq,
        avg(row_seq_2) over (partition by txn_type order by row_seq, id rows between 3 preceding and 1 preceding) as row_seq_2_avg_3_prior,
        count(is_new_burst) over (partition by account_id, txn_type order by cast(txn_time as bigint) range between 7776000 preceding and current row) as is_new_burst_count_90d,
        avg(balance) over (order by txn_time, id rows between 6 preceding and current row) as balance_avg_6,
        lag(event_seq, 2, 0) over (partition by account_type order by event_seq_avg_90d, id) as prev_event_seq
    from scored_transactions
),
with_dims_transactions as (
    -- Attach accounts attributes
    select
        s.*,
        b.opened_date,
        b.balance as account_balance,
        b.status as account_status
    from metrics s
    left join accounts b
        on s.account_id = b.id
)
-- Multi-level aggregation
select
    account_status,
    case when is_new_burst_count_90d >= 18 then 'HIGH' else 'LOW' end as is_new_burst_count_90d_tier,
    count_if(account_status = 'FROZEN') as frozen_events,
    count(*) as row_count,
    avg(account_balance) filter (where account_type = 'SAVINGS') as savings_avg_account_balance,
    count(distinct account_id) as distinct_account_ids,
    size(collect_set(status)) as num_status,
    percent_rank() over (partition by account_status order by count_if(account_status = 'FROZEN') desc) as frozen_events_percent_rank
from with_dims_transactions
group by 1, 2
having num_status < 1000
order by account_status desc;
