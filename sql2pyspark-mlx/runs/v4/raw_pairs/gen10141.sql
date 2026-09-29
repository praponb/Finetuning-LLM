select country, browser
from sessions
where customer_id is null
union all
select country, browser
from sessions
where customer_id is null
