select is_active, lower(email) as email_lower
from customers
where signup_date >= '2025-03-01'
order by email_lower desc;
