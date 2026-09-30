select min(amount) as min_amount, count(distinct amount) as unique_merchants from transactions;
