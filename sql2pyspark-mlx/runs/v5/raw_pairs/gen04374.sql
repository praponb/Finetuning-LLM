select quarter(c.signup_date) as signup_quarter, sort_array(collect_set(o.channel)) as channel_list
from orders o
right join customers c on c.id = o.customer_id
group by quarter(c.signup_date)
