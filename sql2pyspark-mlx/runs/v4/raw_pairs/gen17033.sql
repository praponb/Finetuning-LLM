select
    case when credit_limit >= 16200 then 'PREMIUM' else 'STANDARD' end as credit_limit_tier,
    country,
    date_trunc('year', signup_date) as year_start,
    sum(case when country = 'JP' then credit_limit else 0 end) as jp_credit_limit,
    count(*) filter (where country = 'UK') as uk_rows
from customers
group by case when credit_limit >= 16200 then 'PREMIUM' else 'STANDARD' end, country, date_trunc('year', signup_date)
order by year_start desc nulls first;
