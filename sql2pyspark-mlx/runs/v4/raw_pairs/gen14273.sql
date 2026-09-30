select *, initcap(trim(country)) as country_title, date_trunc('MONTH', started_at) as started_at_period
from sessions
where hour(started_at) between 10 and 16 and customer_id in (2, 14, 15);
