with enriched as (
    select
        *,
        lag(balance) over w_grp as prev_balance,
        lead(balance) over w_grp as next_balance,
        row_number() over w_grp as event_seq
    from accounts
    where customer_id = 31
    window w_grp as (partition by account_type, status order by opened_date, id)
),
scored as (
    select
        *,
        lead(balance) over (partition by status, customer_id order by opened_date, id) as next_balance_2,
        lag(next_balance) over (partition by status, customer_id order by opened_date, id) as prev_next_balance,
        row_number() over (partition by status, customer_id order by opened_date, id) as seq_no,
        lag(balance, 1) over (partition by customer_id, status order by event_seq, id) as prev_balance_2
    from enriched
),
base as (
    select *, case when account_type in ('SAVINGS', 'LOAN') then 'Tier 1' else 'Other' end as account_type_group
    from scored
    where account_type in ('SAVINGS', 'CREDIT', 'CHECKING')
      and status is distinct from 'FROZEN'
),
scored_accounts as (
    select
        id,
        customer_id,
        account_type,
        opened_date,
        next_balance,
        next_balance_2,
        seq_no,
        account_type_group,
        min(prev_balance_2) over w_grp as last7_min_prev_balance_2,
        avg(prev_balance_2) over w_grp as last7_avg_prev_balance_2,
        sum(prev_balance_2) over w_grp as last7_sum_prev_balance_2,
        ntile(3) over w_part as event_seq_bucket
    from base
    window w_grp as (partition by account_type, status order by event_seq, id rows between 6 preceding and current row), w_part as (order by event_seq desc, id)
)
select
    customer_id,
    date_trunc('week', opened_date) as week_start,
    round(sum(next_balance) / nullif(count(case when account_type = 'CHECKING' then 1 end), 0), 2) as next_balance_per_checking,
    count(distinct account_type) as distinct_account_types,
    count(case when account_type = 'SAVINGS' then 1 end) as savings_count,
    sum(case when account_type = 'SAVINGS' then event_seq_bucket else 0 end) as savings_event_seq_bucket,
    round(avg(next_balance_2), 2) as avg_next_balance_2,
    sum(seq_no) filter (where account_type = 'CHECKING') as checking_total_seq_no,
    grouping_id() as gid
from scored_accounts
group by cube (customer_id, date_trunc('week', opened_date))
order by customer_id desc;
