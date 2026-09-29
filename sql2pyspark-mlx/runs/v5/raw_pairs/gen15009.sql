select status, start_date, quarter(end_date) as end_date_quarter from subscriptions where customer_id = 30 and (end_date < '2023-11-01' or end_date >= '2023-04-01') order by start_date desc;
