select month(end_date) as end_month, min(monthly_fee) as min_monthly_fee from subscriptions group by end_month having min(monthly_fee) >= 180;
