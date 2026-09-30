select
    a.id,
    a.customer_id,
    a.account_type,
    a.opened_date,
    c.signup_date,
    c.is_active
from accounts a
full outer join customers c on c.id = a.customer_id
where c.age >= 65 or c.credit_limit is not null
order by id desc, signup_date asc;
