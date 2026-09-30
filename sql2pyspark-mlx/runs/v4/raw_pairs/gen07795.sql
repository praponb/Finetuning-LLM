select * from payments where amount >= 1180 or order_id is not null order by amount desc;
