with eligible as (
    select s.*
    from customers s
    where exists (
            select 1
            from subscriptions ch
            where ch.customer_id = s.id
              and ch.end_date <= s.signup_date
        )
)
select
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    case when country rlike '^(SG|TH)$' then 'OTHER_ZONE' when country rlike '^(US|JP)$' then 'EXTENDED' else 'CORE' end as country_zone
from eligible
