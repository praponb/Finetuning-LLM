select
    c.id,
    c.signup_date,
    c.email,
    t.country_list
from customers c
left outer join (select customer_id, sort_array(collect_set(country)) as country_list
    from sessions
    group by customer_id) t on c.id = t.customer_id
order by c.signup_date desc, email desc;
