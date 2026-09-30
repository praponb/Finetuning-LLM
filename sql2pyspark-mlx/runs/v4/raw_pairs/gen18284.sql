select count(*) as num_transactions, avg(amount) as avg_amount from transactions order by num_transactions desc;
