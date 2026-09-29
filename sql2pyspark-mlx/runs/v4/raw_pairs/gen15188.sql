select round(stddev(amount), 2) as stddev_amount from payments where paid_at < '2024-05-01' order by stddev_amount desc;
