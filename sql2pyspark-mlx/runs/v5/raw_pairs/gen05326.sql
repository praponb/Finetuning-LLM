select is_active, max(credit_limit) - min(credit_limit) as credit_limit_range
from customers
where credit_limit between 10500 and 11700 and segment != 'Consumer' and name is not null
group by is_active
