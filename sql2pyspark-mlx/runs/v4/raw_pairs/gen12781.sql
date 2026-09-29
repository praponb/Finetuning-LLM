select launch_date, date_format(launch_date, 'yyyy-MM') as launch_date_label, case when price >= 1180 then 'High' when price >= 350 then 'Medium' else 'Low' end as price_level
from products
where cost < 440 and cost <= 510
order by launch_date
