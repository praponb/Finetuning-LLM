select date_format(launch_date, 'yyyy-MM') as launch_month, count(case when category = 'Books' then 1 end) as books_count
from products
group by date_format(launch_date, 'yyyy-MM')
order by books_count asc, launch_month asc;
