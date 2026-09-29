select plan, start_date, end_date, add_months(end_date, 1) as end_date_plus_months from subscriptions where customer_id is not null or end_date >= '2023-07-01' order by plan desc, end_date;
