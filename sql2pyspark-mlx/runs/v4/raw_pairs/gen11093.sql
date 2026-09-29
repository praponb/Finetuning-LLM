select date_trunc('MONTH', launch_date) as launch_month, sum(case when brand = 'Stark' then 1 else 0 end) as stark_count from products group by 1
