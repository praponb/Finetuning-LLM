select year(launch_date) as launch_year, min(launch_date) as first_launch_date, count(*) as cnt from products group by year(launch_date) order by cnt;
