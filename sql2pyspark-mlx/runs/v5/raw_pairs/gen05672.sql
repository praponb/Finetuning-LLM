select method, sum(amount) as total_amount, round(sum(amount) * 100.0 / sum(sum(amount)) over (), 2) as pct_of_total from payments group by method order by total_amount desc
