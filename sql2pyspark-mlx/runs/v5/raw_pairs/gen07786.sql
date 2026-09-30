with eligible as (
    select s.*
    from customers s
    where exists (
            select 1
            from sessions c
            where c.customer_id = s.id
              and c.device != 'desktop'
        )
),
with_latest as (
    select s.*, top1.account_type
    from eligible s
    join lateral (
            select c.account_type
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) top1 on true
),
with_latest_customers as (
    select
        s.*,
        latest.device,
        latest.session_started_at
    from with_latest s
    left join lateral (
            select c.device, c.started_at as session_started_at
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) latest on true
),
enriched as (
    select
        id,
        name,
        email,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        device,
        corr(age, credit_limit) over (partition by device, segment) corr_age_credit_limit,
        avg(credit_limit) over w_recent credit_limit_avg_2,
        corr(age, credit_limit) over (partition by device, account_type) corr_age_credit_limit_2
    from with_latest_customers
    where device = 'mobile'
    window w_recent as (partition by city order by credit_limit asc, id asc rows between 2 preceding and current row)
),
deduped as (
    select
        id,
        name,
        is_active,
        credit_limit,
        age,
        credit_limit_avg_2,
        corr_age_credit_limit_2
    from (
        select *, row_number() over (partition by city order by corr_age_credit_limit desc nulls last, id asc) as latest_rank
        from enriched
    ) t
    where latest_rank = 1
)
select *
from (
    select
        id,
        credit_limit,
        corr_age_credit_limit_2
    from deduped
) src
unpivot (
    value for metric_name in (credit_limit, corr_age_credit_limit_2)
);
