select round(avg(amount), 2) as avg_amount, sum(case when merchant = 'Amazon' then amount else 0 end) as amazon_amount from transactions;
