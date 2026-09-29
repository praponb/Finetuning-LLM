select category, date_trunc('MONTH', launch_date) as launch_month, round(avg(cost), 2) as avg_cost from products group by category, date_trunc('MONTH', launch_date);
