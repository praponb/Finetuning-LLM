select segment, is_active, count(distinct credit_limit) unique_segments, count(*) num_customers from customers group by rollup(segment, is_active) order by num_customers desc
