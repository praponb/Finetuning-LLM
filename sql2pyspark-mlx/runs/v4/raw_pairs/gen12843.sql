select plan, start_date, monthly_fee from subscriptions where monthly_fee is not null order by plan, monthly_fee desc;
