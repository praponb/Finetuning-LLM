select signup_date
from customers
union
select sale_date
from store_sales
where units = 32;
