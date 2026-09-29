select * from (select date_format(opened_date, 'yyyy-MM') as opened_month, region, sqft from stores) pivot (avg(sqft) for region in ('North', 'West', 'Central')) order by opened_month
