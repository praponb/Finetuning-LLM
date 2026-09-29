select status from accounts where customer_id in (2, 12, 16, 35) intersect select status from payments where order_id is not null;
