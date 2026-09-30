select category, sum(cost) as total_cost, rank() over (order by sum(cost) desc) as rnk from products group by category
