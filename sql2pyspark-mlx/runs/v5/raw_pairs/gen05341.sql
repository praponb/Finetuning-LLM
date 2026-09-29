select launch_date, year(launch_date) as launch_date_year, size(tags) as tags_count
from products
order by launch_date;
