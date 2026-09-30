select id, channel, order_date, status, upper(status) as status_upper from orders where discount between 0.18 and 0.21;
