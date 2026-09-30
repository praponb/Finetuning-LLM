with latest as (
    -- QUALIFY-style window filter
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged
    from (
        select *, row_number() over (partition by merchant order by amount desc nulls first, id) as dedup_rank
        from transactions
    ) ranked
    where dedup_rank = 1
),
joined as (
    -- Attach accounts attributes
    select
        a.id,
        a.account_id,
        a.is_flagged,
        p2.opened_date,
        p2.status
    from latest a
    left join accounts p2
        on a.account_id = p2.id
    where p2.status not in ('CLOSED', 'FROZEN')
),
enriched as (
    -- Attach accounts attributes
    select
        src.*,
        b.status as account_status,
        b.opened_date as account_opened_date,
        b.balance
    from joined src
    inner join accounts b
        on src.account_id = b.id
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by account_status order by opened_date desc nulls last, id) as dedup_rank
        from enriched
    ) x
    where dedup_rank = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        account_id,
        account_opened_date,
        balance,
        dedup_rank,
        last_value(balance, true) over (order by account_opened_date, id rows between unbounded preceding and current row) as last_known_balance,
        row_number() over (partition by account_status, account_id order by balance desc, id) as seq_no,
        lead(balance, 2) over (partition by status, account_status order by opened_date, id) as next_balance
    from ranked
),
remaining_keys as (
    -- Set difference on keys
    select id, account_id
    from scored
    except all
    select id, account_id
    from transactions
    where is_flagged = true
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join remaining_keys k on s.id = k.id and s.account_id = k.account_id
),
enriched_transactions as (
    -- Attach accounts attributes
    select src.*, d.balance as account_balance
    from kept_rows src
    left join accounts d
        on src.account_id = d.id
)
-- Multi-level aggregation
select
    case when balance >= 77000 then 'LARGE' else 'SMALL' end as balance_tier,
    case when account_balance >= 71000 then 'PREMIUM' else 'STANDARD' end as account_balance_tier,
    date_trunc('week', account_opened_date) as week_start,
    count(distinct account_id) as distinct_account_ids,
    count(distinct account_id) as distinct_account_ids_2
from enriched_transactions
group by rollup (case when balance >= 77000 then 'LARGE' else 'SMALL' end, case when account_balance >= 71000 then 'PREMIUM' else 'STANDARD' end, date_trunc('week', account_opened_date))
order by balance_tier nulls first
