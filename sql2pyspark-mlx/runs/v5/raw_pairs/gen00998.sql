with cleaned as (
    select *, concat(status, ', ', account_type) as status_account_type
    from accounts
    where opened_date > date '2023-06-19'
),
with_window as (
    select
        s.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from cleaned s
    inner join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.opened_date >= sub.start_date
       and s.opened_date < sub.end_date + interval 14 days
),
valid as (
    select *, substring(account_type, 1, 3) as account_type_prefix
    from with_window
    where customer_id in (31, 40)
),
latest as (
    select
        id,
        balance,
        status,
        subscription_id,
        plan,
        monthly_fee,
        account_type_prefix
    from (
        select *, row_number() over (partition by customer_id, plan order by balance, id, subscription_id) as rank_in_group
        from valid
    ) x
    where rank_in_group = 1
),
not_flagged as (
    select s.*
    from latest s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.account_id in (1, 20, 30)
        )
),
valid_accounts as (
    select
        id,
        status,
        subscription_id,
        plan,
        monthly_fee,
        account_type_prefix,
        lower(status) as status_lower
    from not_flagged
    where plan = 'premium'
),
scored as (
    select
        *,
        lag(monthly_fee) over w_part as prev_monthly_fee,
        lead(subscription_id) over w_part as next_subscription_id,
        lag(subscription_id) over w_part as prev_subscription_id,
        row_number() over w_part as seq_no
    from valid_accounts
    where plan in ('premium', 'standard', 'basic')
    window w_part as (partition by plan order by subscription_id, id, subscription_id)
)
select
    seq_no,
    prev_subscription_id,
    case when subscription_id >= 11 then 'PREMIUM' else 'STANDARD' end as subscription_id_tier,
    approx_percentile(prev_monthly_fee, 0.9) as prev_monthly_fee_p90,
    max(seq_no) filter (where status = 'OPEN') as open_max_seq_no
from scored
group by seq_no, prev_subscription_id, case when subscription_id >= 11 then 'PREMIUM' else 'STANDARD' end
order by open_max_seq_no desc;
