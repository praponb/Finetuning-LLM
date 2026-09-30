select quarter(order_date) as order_quarter, sum(discount) as total_discount, round(sum(discount) * 100.0 / sum(sum(discount)) over (), 2) as pct_of_total from orders group by quarter(order_date);
