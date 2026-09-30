select * from (select category, brand, price from products) pivot (max(price) for brand in ('Stark', 'Initech', 'Globex', 'Umbrella')) order by category;
