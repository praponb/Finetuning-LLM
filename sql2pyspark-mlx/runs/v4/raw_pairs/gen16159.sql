with scored as (
    select
        *,
        count(age) over (partition by segment) segment_count_age,
        percent_rank() over (partition by city, country order by credit_limit desc nulls last) credit_limit_percent_rank
    from customers
),
latest_match as (
    select
        s.*,
        x.account_status,
        x.balance
    from scored s
    left join lateral (
            select c.status as account_status, c.balance
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) x on true
),
with_last as (
    select
        s.*,
        top1.order_amount,
        top1.channel
    from latest_match s
    left join lateral (
            select c.amount as order_amount, c.channel
            from orders c
            where c.customer_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) top1 on true
),
latest as (
    select *, row_number() over (partition by city order by balance nulls last, id) as rank_in_group
    from with_last
    qualify rank_in_group = 1
)
select
    id,
    name,
    email,
    country,
    segment,
    is_active,
    credit_limit,
    age,
    segment_count_age,
    case when segment rlike '^(Consumer|SMB)$' then 'EXTENDED' when segment rlike '^(Corporate)$' then 'PRIORITY' else 'GROUP_B' end as segment_zone
from latest
order by segment_zone desc nulls first;
