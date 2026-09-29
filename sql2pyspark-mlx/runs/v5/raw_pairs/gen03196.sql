with filtered as (
    -- Keep recent, valid rows
    select
        *,
        round(balance, 0) as balance_rounded,
        case when status = 'FROZEN' then 1 else 0 end as is_frozen,
        coalesce(status, 'NONE') as status_clean
    from accounts
    where customer_id is not null
),
with_latest as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.account_type,
        s.balance,
        s.status,
        s.status_clean,
        top1.transaction_txn_type
    from filtered s
    left join lateral (
            select c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.account_type,
        s.status_clean,
        top1.transaction_merchant
    from with_latest s
    left join lateral (
            select c.merchant as transaction_merchant
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
),
with_last_accounts as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.merchant,
        top1.amount
    from with_last s
    join lateral (
            select c.merchant, c.amount
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
stats as (
    -- Grouped metrics
    select
        case when amount >= 3900 then 'PREMIUM' else 'STANDARD' end as amount_tier,
        count(case when merchant = 'Shell' then 1 end) as shell_count,
        count(*) as num_rows,
        size(collect_set(merchant)) as num_merchants
    from with_last_accounts
    group by case when amount >= 3900 then 'PREMIUM' else 'STANDARD' end
)
select amount_tier, shell_count
from stats
order by amount_tier desc;
