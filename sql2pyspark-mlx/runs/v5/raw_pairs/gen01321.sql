select status
from orders
where channel != 'mobile'
union
select status
from accounts;
