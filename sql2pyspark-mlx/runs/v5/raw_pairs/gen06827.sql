select
    id,
    brand,
    launch_date,
    percent_rank() over (partition by brand order by launch_date) as launch_date_pct_rank
from products
where price between 380 and 1190
order by id desc, launch_date desc;
