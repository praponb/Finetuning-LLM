select id, customer_id, start_date, row_number() over (partition by customer_id order by start_date, id) as start_date_row from subscriptions order by start_date_row;
