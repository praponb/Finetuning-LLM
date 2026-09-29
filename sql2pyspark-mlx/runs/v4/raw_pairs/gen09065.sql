select id, start_date, row_number() over (order by start_date desc, id) as start_date_row from subscriptions where plan != 'standard' order by id desc, start_date desc;
