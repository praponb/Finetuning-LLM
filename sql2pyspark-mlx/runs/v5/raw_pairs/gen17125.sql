select max(signup_date) as latest_signup_date, avg(age) as avg_age, sort_array(collect_set(segment)) as segment_list
from customers
where credit_limit is null and segment not in ('Consumer', 'SMB')
order by 2 desc
