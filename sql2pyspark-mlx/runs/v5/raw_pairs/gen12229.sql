select id, status, order_date, row_number() over (partition by status order by order_date desc, id) as row_num from orders;
