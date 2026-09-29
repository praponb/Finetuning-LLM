select year(opened_date) as opened_year, count(case when status = 'OPEN' then 1 end) as open_count
from accounts
where customer_id in (3, 25, 32, 35)
group by opened_year
