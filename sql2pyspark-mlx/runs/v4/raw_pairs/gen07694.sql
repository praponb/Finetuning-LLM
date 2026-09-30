select month(opened_date) as opened_month, count(sqft) as sqft_count from stores where region = 'Central' group by opened_month;
