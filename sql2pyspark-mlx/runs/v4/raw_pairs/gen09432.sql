select *, upper(country) as country_upper
from sessions
where browser in ('Edge', 'Safari', 'Firefox') and customer_id in (23, 28) and browser is null
