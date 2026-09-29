select method, round(avg(amount), 2) avg_amount from payments group by 1 having round(avg(amount), 2) > 31 order by avg_amount, method limit 20;
