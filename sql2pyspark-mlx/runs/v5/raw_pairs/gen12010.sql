with valid as (
    -- Keep recent, valid rows
    select *, cast(is_flagged as int) as is_flagged_flag
    from transactions
    where merchant is not null
),
enriched as (
    -- Attach accounts attributes
    select s.*, d.account_type
    from valid s
    inner join accounts d
        on s.account_id = d.id
    where d.opened_date between '2023-12-01' and '2025-07-01'
),
joined as (
    -- Attach accounts attributes
    select
        a.*,
        p.status,
        p.balance
    from enriched a
    left join accounts p
        on a.account_id = p.id
),
joined_transactions as (
    -- Attach accounts attributes
    select
        s.*,
        p.status as account_status,
        p.account_type as account_account_type
    from joined s
    left join accounts p
        on s.account_id = p.id
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by account_status order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by account_status order by txn_time, id) + interval 60 days then 1 else 0 end as is_new_burst
    from joined_transactions
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_flag,
        account_type,
        status,
        balance,
        account_status,
        sum(is_new_burst) over (partition by account_status order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
)
-- Native SQL PIVOT
select *
from (
    select
        status,
        merchant,
        burst_id
    from sessionized
) src
pivot (
    sum(burst_id) as value_sum,
    count(1) as cnt
    for merchant in ('Shell' as shell, 'Starbucks' as starbucks, 'Amazon' as amazon)
);
