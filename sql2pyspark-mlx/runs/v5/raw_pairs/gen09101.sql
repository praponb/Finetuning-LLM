with metrics as (
    -- Running and moving aggregates
    select
        id,
        account_id,
        merchant,
        count(amount) over (partition by txn_type) as txn_type_count_amount,
        last_value(merchant, true) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as last_known_merchant,
        lag(amount, 1) over (order by txn_time, id) as prev_amount,
        percent_rank() over (order by amount desc nulls last) as amount_percent_rank
    from transactions
),
joined as (
    -- Attach accounts attributes
    select a.*, b.opened_date
    from metrics a
    left join accounts b
        on a.account_id = b.id
    where b.account_type != 'CHECKING'
),
enriched as (
    -- Attach accounts attributes
    select
        s.*,
        d.account_type,
        d.balance,
        d.status
    from joined s
    left join accounts d
        on s.account_id = d.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        account_id,
        merchant,
        txn_type_count_amount,
        last_known_merchant,
        prev_amount,
        amount_percent_rank,
        opened_date,
        account_type,
        status,
        datediff(current_date(), opened_date) as days_since_opened_date,
        coalesce(account_type, 'DIRECT') as account_type_filled
    from enriched
    where prev_amount is not null
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by account_id, merchant order by days_since_opened_date desc nulls last) as row_num
        from cleaned
    ) t
    where row_num = 1
)
select
    s.id,
    s.account_id,
    s.merchant,
    s.txn_type_count_amount,
    s.amount_percent_rank,
    s.opened_date,
    s.account_type,
    s.days_since_opened_date,
    s.account_type_filled,
    case when account_type rlike '^(CREDIT|SAVINGS)$' then 'GROUP_A' when account_type rlike '^(CHECKING|LOAN)$' then 'EXTENDED' else 'GROUP_B' end as account_type_zone,
    (select sum(t2.amount) from transactions t2 where t2.account_id = s.account_id) as sum_transaction_amount
from ranked s
order by sum_transaction_amount nulls last, id
