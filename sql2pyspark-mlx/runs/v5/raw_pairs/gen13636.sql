select id, category, launch_date, cost, lead(cost, 1) over (partition by category order by launch_date, id) as next_cost from products where launch_date between '2024-10-01' and '2025-09-01'
