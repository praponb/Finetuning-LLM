select * from (select supplier_id, category, price from products) pivot (sum(price) for category in ('Books', 'Fashion', 'Grocery', 'Sports')) order by supplier_id
