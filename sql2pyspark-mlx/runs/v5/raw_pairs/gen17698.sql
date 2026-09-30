select status, year(opened_date) as opened_year, min(opened_date) as first_opened_date
from accounts
where balance < 78000 and opened_date > '2024-12-01'
group by 1, 2
order by first_opened_date desc, status desc;
