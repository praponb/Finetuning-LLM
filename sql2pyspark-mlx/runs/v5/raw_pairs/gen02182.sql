select year(c.signup_date) as signup_year, case when c.city in ('New York', 'Berlin') then 'core' else 'other' end as city_group, sort_array(collect_set(a.status)) as status_list
from accounts a
right join customers c on a.customer_id = c.id
where a.status in ('OPEN', 'CLOSED')
group by year(c.signup_date), case when c.city in ('New York', 'Berlin') then 'core' else 'other' end;
