with metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(balance) over w_running as running_sum_balance,
        max(balance) over w_running as running_max_balance,
        avg(balance) over w_running as running_avg_balance,
        count(balance) over w_running as running_count_balance
    from accounts
    where balance <= 51000
    window w_running as (partition by status order by balance, id rows between unbounded preceding and current row)
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        account_type,
        balance,
        opened_date,
        running_sum_balance,
        running_max_balance,
        running_avg_balance,
        dense_rank() over (partition by status order by running_sum_balance nulls first) as running_sum_balance_dense_rank,
        sum(running_max_balance) over w_part as status_sum_running_max_balance,
        lag(running_count_balance, 1) over (partition by customer_id, status order by balance, id) as prev_running_count_balance,
        min(running_max_balance) over w_grp as account_type_min_running_max_balance
    from metrics
    window w_part as (partition by status), w_grp as (partition by account_type, customer_id)
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_windows s
    where exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time < s.opened_date
        )
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        account_type,
        running_sum_balance,
        running_avg_balance,
        status_sum_running_max_balance,
        prev_running_count_balance,
        account_type_min_running_max_balance,
        lead(account_type_min_running_max_balance) over w_running as next_account_type_min_running_max_balance,
        lead(prev_running_count_balance) over w_running as next_prev_running_count_balance,
        lag(prev_running_count_balance) over w_running as prev_prev_running_count_balance,
        row_number() over w_running as seq_no,
        row_number() over (partition by account_type order by running_max_balance desc, id) as seq_no_2
    from not_flagged
    window w_running as (partition by account_type order by running_max_balance, id)
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lead(prev_running_count_balance, 1) over (order by seq_no, id) as next_prev_running_count_balance_2,
        avg(seq_no_2) over (partition by account_type order by account_type_min_running_max_balance, id rows between 5 preceding and current row) as seq_no_2_avg_5,
        min(status_sum_running_max_balance) over w_hist as account_type_min_status_sum_running_max_balance
    from scored
    window w_hist as (partition by account_type)
)
select
    id,
    account_type,
    running_sum_balance,
    account_type_min_running_max_balance,
    next_account_type_min_running_max_balance,
    next_prev_running_count_balance,
    seq_no,
    seq_no_2,
    next_prev_running_count_balance_2
from enriched
order by seq_no nulls first, running_sum_balance;
