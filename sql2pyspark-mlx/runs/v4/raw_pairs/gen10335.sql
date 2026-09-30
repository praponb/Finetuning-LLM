select opened_date from accounts union select order_date as opened_date from orders where amount between 680 and 1570;
