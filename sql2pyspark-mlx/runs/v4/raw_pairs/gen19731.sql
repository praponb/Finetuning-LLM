with kept as (
    select s.*
    from accounts s
    where exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time <= s.opened_date
        )
),
enriched as (
    select
        *,
        last_value(balance, true) over (partition by account_type, status order by balance, id rows between unbounded preceding and current row) last_known_balance,
        lead(balance, 2) over w_hist next_balance,
        avg(balance) over (partition by account_type order by opened_date, id rows between 2 preceding and current row) balance_avg_2,
        sum(balance) over (partition by customer_id order by balance, id rows between unbounded preceding and current row) cumulative_balance
    from kept
    window w_hist as (partition by customer_id, status order by balance, id)
),
with_windows as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        last_known_balance,
        next_balance,
        balance_avg_2,
        cumulative_balance,
        sum(next_balance) over (partition by account_type) account_type_sum_next_balance,
        avg(cumulative_balance) over w_running cumulative_balance_avg_3_prior
    from enriched
    window w_running as (partition by customer_id order by opened_date, id rows between 3 preceding and 1 preceding)
),
deduped as (
    select *
    from with_windows
    qualify rank() over (partition by status, customer_id order by next_balance desc) = 1
),
scored as (
    select
        *,
        row_number() over (partition by status order by last_known_balance desc, id) row_seq,
        sum(next_balance) over (partition by status) status_sum_next_balance,
        sum(account_type_sum_next_balance) over (partition by account_type order by cumulative_balance_avg_3_prior, id rows between unbounded preceding and current row) running_account_type_sum_next_balance
    from deduped
),
joined as (
    select a.*, d.age
    from scored a
    left join customers d
        on a.customer_id = d.id
),
stats as (
    select
        date_trunc('quarter', opened_date) as quarter_start,
        count(*) filter (where account_type = 'LOAN') as loan_rows,
        sum(case when status = 'FROZEN' then account_type_sum_next_balance else 0 end) as frozen_account_type_sum_next_balance,
        sum(case when account_type = 'LOAN' then status_sum_next_balance else 0 end) as loan_status_sum_next_balance,
        count(*) filter (where status = 'CLOSED') as closed_rows,
        count(distinct account_type) as distinct_account_types
    from joined
    group by quarter_start
    having distinct_account_types >= 1
)
select
    quarter_start,
    loan_rows,
    frozen_account_type_sum_next_balance,
    loan_status_sum_next_balance,
    closed_rows,
    distinct_account_types
from stats
