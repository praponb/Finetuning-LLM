select id, signup_date
from customers
where signup_date = (select min(signup_date) from customers)
