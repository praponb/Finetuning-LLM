select date_format(c.signup_date, 'yyyy-MM') as signup_month, sort_array(collect_set(o.status)) as status_list
from orders o
right outer join stores s on o.store_id = s.id
inner join customers c on o.customer_id = c.id
group by date_format(c.signup_date, 'yyyy-MM')
order by signup_month;
