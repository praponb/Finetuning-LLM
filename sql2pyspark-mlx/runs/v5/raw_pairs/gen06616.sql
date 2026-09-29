select order_quarter, discount_count from (select quarter(order_date) order_quarter, count(discount) discount_count from orders group by quarter(order_date)) agg where discount_count < 8;
