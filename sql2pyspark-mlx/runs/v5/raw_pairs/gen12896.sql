select
    id,
    name,
    city,
    credit_limit * age credit_limit_times_age
from customers
where not is_active and (country in ('SG', 'UK') or segment in ('Corporate', 'Consumer'))
order by id, name desc;
