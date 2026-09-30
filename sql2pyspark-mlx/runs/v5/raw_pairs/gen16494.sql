select date_format(opened_date, 'yyyy-MM') as opened_month, sum(sqft) as total_sqft from stores where region = 'North' group by date_format(opened_date, 'yyyy-MM') order by opened_month;
