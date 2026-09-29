select min(monthly_fee) as min_monthly_fee, round(stddev(monthly_fee), 2) as stddev_monthly_fee from subscriptions;
