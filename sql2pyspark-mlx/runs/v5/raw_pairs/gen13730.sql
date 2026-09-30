select signup_year, pct_consumer
from (
    select year(signup_date) signup_year, round((100 * sum(case when segment = 'Consumer' then 1 else 0 end)) / count(*), 1) pct_consumer
    from customers
    group by year(signup_date)
) agg
where pct_consumer < 31
