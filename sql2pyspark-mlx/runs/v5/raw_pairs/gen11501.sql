select email
from customers
where credit_limit < 15200
except
select email
from employees
where is_remote = true;
