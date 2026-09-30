select signup_month, city_list, consumer_credit_limit
from (
    select month(signup_date) as signup_month, sort_array(collect_set(city)) as city_list, sum(case when segment = 'Consumer' then credit_limit else 0 end) as consumer_credit_limit
    from customers
    group by month(signup_date)
) as s
where consumer_credit_limit >= 36
order by consumer_credit_limit desc;
