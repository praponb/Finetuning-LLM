select method, sum(amount) as total_amount, count(*) as cnt, round(sum(amount) * 100.0 / sum(sum(amount)) over (), 2) as pct_of_total from payments group by method
