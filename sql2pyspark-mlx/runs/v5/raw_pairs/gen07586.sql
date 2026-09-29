select * from (select status, count(distinct monthly_fee) as unique_customers from subscriptions group by status) as x where unique_customers < 8;
