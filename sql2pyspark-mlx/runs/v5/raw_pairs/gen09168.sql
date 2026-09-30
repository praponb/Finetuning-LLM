select category, sum(cost) as total_cost, count(*) as cnt, sum(sum(cost)) over (order by category) as cumulative_cost from products group by category order by cumulative_cost desc;
