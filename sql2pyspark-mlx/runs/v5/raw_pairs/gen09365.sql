select *
from (
    select
        country,
        case when credit_limit >= 17900 then 'high' when credit_limit >= 4700 then 'mid' else 'low' end as credit_limit_bucket,
        count(age) as age_count,
        min(signup_date) as min_signup_date
    from customers
    group by country, case when credit_limit >= 17900 then 'high' when credit_limit >= 4700 then 'mid' else 'low' end
) as sub
where age_count >= 7;
