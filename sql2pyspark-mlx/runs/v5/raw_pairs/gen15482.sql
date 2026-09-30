select country, email, credit_limit
from customers
where age between 33 and 41 or email not like '%@yahoo.com'
