select * from customers qualify rank() over (partition by segment order by credit_limit nulls last) = 1;
