select segment, sum(case when country = 'UK' then 1 else 0 end) as uk_count, avg(credit_limit) as avg_credit_limit, max(credit_limit) as max_credit_limit from customers group by 1
