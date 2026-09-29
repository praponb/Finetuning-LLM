with with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select s.*, top1.order_channel
    from customers s
    join lateral (
            select c.channel as order_channel
            from orders c
            where c.customer_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) top1 on true
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        email,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        order_channel,
        upper(email) as email_upper,
        datediff(current_date(), signup_date) as days_since_signup_date
    from with_last
    where signup_date >= date '2024-03-09'
      and credit_limit is null
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from subscriptions c
            where c.customer_id = s.id
              and c.start_date >= s.signup_date
        )
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(segment, 'unassigned') as segment_filled,
        coalesce(segment, 'UNKNOWN') as segment_clean
    from eligible
    where order_channel is not null
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from cleaned s
    where not exists (
            select 1
            from sessions ch
            where ch.customer_id = s.id
              and ch.browser in ('Safari', 'Chrome')
        )
),
cleaned_customers as (
    -- Keep recent, valid rows
    select
        *,
        add_months(signup_date, 1) as signup_date_plus_months,
        coalesce(segment, 'N/A') as segment_filled_2
    from not_flagged
    where signup_date > date '2023-04-29'
),
cleaned_customers_2 as (
    -- Keep recent, valid rows
    select
        id,
        email,
        segment,
        signup_date,
        credit_limit,
        age,
        email_upper,
        days_since_signup_date,
        segment_clean,
        signup_date_plus_months,
        cast(age as double) as age_str,
        lower(order_channel) as order_channel_lower
    from cleaned_customers
    where is_active = false
)
select
    id,
    segment,
    signup_date,
    credit_limit,
    age,
    days_since_signup_date,
    segment_clean,
    age_str,
    order_channel_lower,
    case when segment rlike '^(SMB|Corporate)$' then 'EXTENDED' when segment rlike '^(Consumer)$' then 'GROUP_B' else 'PRIORITY' end as segment_zone
from cleaned_customers_2
order by segment_zone desc
