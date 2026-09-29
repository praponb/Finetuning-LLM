with filtered as (
    select
        *,
        coalesce(monthly_fee, 0) as monthly_fee_filled,
        case when status = 'EXPIRED' then 1 else 0 end as is_expired
    from subscriptions
    where end_date > date '2023-09-04'
      and customer_id is not null
      and monthly_fee > 130
),
with_windows as (
    select
        *,
        stddev_samp(monthly_fee) over (partition by customer_id, plan) as customer_id_stddev_samp_monthly_fee,
        round(monthly_fee / sum(monthly_fee) over (partition by status), 4) as monthly_fee_share,
        ntile(10) over (order by is_expired desc, id) as is_expired_bucket,
        avg(monthly_fee) over (order by monthly_fee_filled, id rows between 3 preceding and current row) as monthly_fee_avg_3
    from filtered
),
with_windows_subscriptions as (
    select
        *,
        lag(customer_id_stddev_samp_monthly_fee, 2) over (partition by plan order by monthly_fee_share, id) as prev_customer_id_stddev_samp_monthly_fee,
        first_value(is_expired_bucket) ignore nulls over (partition by status order by is_expired, id rows between unbounded preceding and current row) as first_known_is_expired_bucket,
        round(is_expired_bucket / sum(is_expired_bucket) over (partition by status, customer_id), 4) as is_expired_bucket_share
    from with_windows
),
scored as (
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_filled,
        is_expired,
        customer_id_stddev_samp_monthly_fee,
        monthly_fee_share,
        is_expired_bucket,
        monthly_fee_avg_3,
        prev_customer_id_stddev_samp_monthly_fee,
        first_known_is_expired_bucket,
        is_expired_bucket_share,
        avg(first_known_is_expired_bucket) over (partition by customer_id order by prev_customer_id_stddev_samp_monthly_fee, id rows between 2 preceding and 1 preceding) as first_known_is_expired_bucket_avg_2_prior,
        ntile(10) over (partition by customer_id, plan order by customer_id_stddev_samp_monthly_fee, id) as customer_id_stddev_samp_monthly_fee_bucket,
        last_value(monthly_fee_share, true) over (order by monthly_fee_share, id rows between unbounded preceding and current row) as last_known_monthly_fee_share
    from with_windows_subscriptions
),
deduped as (
    select *
    from (
        select *, rank() over (partition by customer_id order by monthly_fee_filled nulls first) as rn
        from scored
    ) ranked
    where rn = 1
),
metrics as (
    select
        *,
        round(customer_id_stddev_samp_monthly_fee / sum(customer_id_stddev_samp_monthly_fee) over (partition by status), 4) as customer_id_stddev_samp_monthly_fee_share,
        avg(monthly_fee_avg_3) over (partition by plan order by first_known_is_expired_bucket, id rows between 6 preceding and 1 preceding) as monthly_fee_avg_3_avg_6_prior,
        sum(monthly_fee_filled) over (partition by plan, customer_id order by monthly_fee, id rows between unbounded preceding and current row) as cumulative_monthly_fee_filled
    from deduped
),
enriched as (
    select
        e.*,
        c.signup_date,
        c.segment
    from metrics e
    left join customers c
        on e.customer_id = c.id
)
select
    s.id,
    s.customer_id,
    s.plan,
    s.end_date,
    s.monthly_fee,
    s.monthly_fee_filled,
    s.is_expired,
    s.customer_id_stddev_samp_monthly_fee,
    s.monthly_fee_share,
    case when plan rlike '^(premium|enterprise)$' then 'OTHER_ZONE' when plan rlike '^(basic|standard)$' then 'EXTENDED' else 'GROUP_B' end as plan_zone,
    (select avg(x.balance) from accounts x where x.customer_id = s.customer_id) as avg_account_balance
from enriched s
order by end_date;
