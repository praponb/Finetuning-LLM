select category, avg(cost) as avg_cost, count(price) as price_count, max(cost) as max_cost from products group by 1 having count(case when brand = 'Umbrella' then 1 end) > 4 order by category desc
