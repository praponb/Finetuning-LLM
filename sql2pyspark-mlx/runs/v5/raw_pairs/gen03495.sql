select status, round(stddev(amount), 2) as stddev_amount, round(avg(amount), 2) as avg_amount from payments group by status order by avg_amount desc
