select count(monthly_fee) as monthly_fee_count, min(monthly_fee) as min_monthly_fee, max(end_date) as last_end_date from subscriptions;
