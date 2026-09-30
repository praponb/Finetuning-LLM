select * from (select category, brand, cost from products) pivot (max(cost) for brand in ('Globex', 'Umbrella'));
