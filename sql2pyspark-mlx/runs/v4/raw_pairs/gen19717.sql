select *, case when segment = 'Corporate' then 1 else 0 end is_corporate
from customers;
