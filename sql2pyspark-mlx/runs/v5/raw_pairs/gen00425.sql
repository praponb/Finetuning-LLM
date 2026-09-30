select round(avg(amount), 2) avg_amount, round(stddev(amount), 2) stddev_amount from payments where method = 'bank_transfer' and status <> 'FAILED' order by avg_amount
