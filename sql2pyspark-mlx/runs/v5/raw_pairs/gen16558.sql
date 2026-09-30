with kept_keys as (
    -- Set difference on keys
    select account_type, customer_id
    from accounts
    except all
    select account_type, customer_id
    from accounts
    where account_type not in ('CREDIT', 'SAVINGS', 'CHECKING')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from accounts s
    inner join kept_keys k on s.account_type = k.account_type and s.customer_id = k.customer_id
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from reconciled s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time < s.opened_date
        )
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        avg(balance) over (partition by customer_id, status) as customer_id_avg_balance,
        stddev_samp(balance) over (partition by account_type, customer_id) as account_type_stddev_samp_balance,
        dense_rank() over (order by balance desc nulls first) as balance_dense_rank,
        count(balance) over (partition by customer_id) as customer_id_count_balance
    from eligible
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        sum(account_type_stddev_samp_balance) over (partition by customer_id, status) as customer_id_sum_account_type_stddev_samp_balance,
        rank() over w_part as balance_rank,
        percent_rank() over w_part_2 as account_type_stddev_samp_balance_percent_rank,
        round(balance_dense_rank / sum(balance_dense_rank) over (partition by customer_id), 4) as balance_dense_rank_share,
        min(customer_id_avg_balance) over (partition by customer_id) as customer_id_min_customer_id_avg_balance
    from with_windows
    window w_part as (order by balance desc), w_part_2 as (partition by status, customer_id order by account_type_stddev_samp_balance asc)
)
-- Aggregate with conditional metrics
select
    date_trunc('week', opened_date) as week_start,
    case when balance_rank >= 7 then 'PREMIUM' else 'STANDARD' end as balance_rank_tier,
    date_trunc('quarter', opened_date) as quarter_start,
    count(case when account_type = 'LOAN' then 1 end) as loan_count,
    sort_array(collect_set(account_type)) as account_type_list,
    count(*) as cnt
from scored
group by week_start, balance_rank_tier, quarter_start
order by balance_rank_tier asc nulls first;
