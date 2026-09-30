with with_last as (
    select s.*, top1.account_type
    from customers s
    join lateral (
            select c.account_type
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) top1 on true
)
select
    account_type,
    date_trunc('year', signup_date) as year_start,
    case when credit_limit >= 13700 then 'HIGH' else 'LOW' end as credit_limit_tier,
    count(*) as event_count,
    size(collect_set(account_type)) as num_account_types,
    size(collect_set(account_type)) as num_account_types_2,
    grouping_id() as grouping_level
from with_last
group by grouping sets ((account_type, date_trunc('year', signup_date), case when credit_limit >= 13700 then 'HIGH' else 'LOW' end), (account_type, date_trunc('year', signup_date)), ());
