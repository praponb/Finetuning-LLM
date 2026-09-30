with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        add_months(opened_date, 1) opened_date_plus_months,
        cast(balance as int) balance_int
    from accounts
    where opened_date >= to_date('2023-09-09')
),
with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, x.transaction_txn_time
    from valid s
    join lateral (
            select c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by customer_id order by opened_date_plus_months, id) as rn
    from with_last
    qualify rn = 1
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        opened_date_plus_months,
        balance_int,
        lead(balance) over w_part next_balance,
        lead(balance_int) over w_part next_balance_int,
        row_number() over w_part row_seq
    from ranked
    window w_part as (partition by account_type order by opened_date, id)
),
attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from with_windows e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.opened_date_plus_months >= sub.start_date
       and e.opened_date_plus_months <= date_add(sub.end_date, 7)
)
select
    s.id,
    s.customer_id,
    s.opened_date_plus_months,
    s.next_balance,
    s.next_balance_int,
    s.row_seq,
    s.subscription_id,
    s.plan,
    case when plan rlike '^(enterprise|standard)$' then 'GROUP_B' when plan rlike '^(basic|premium)$' then 'EXTENDED' else 'GROUP_A' end as plan_zone,
    (select max(t2.amount) from orders t2 where t2.customer_id = s.customer_id) as max_order_amount
from attributed s
order by next_balance_int nulls first;
