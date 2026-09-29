select supplier_id, quarter(launch_date) as launch_quarter, category, count(cost) as cost_count from products where brand = 'Acme' group by supplier_id, quarter(launch_date), category
