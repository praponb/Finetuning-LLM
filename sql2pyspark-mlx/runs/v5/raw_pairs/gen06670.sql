select id, supplier_id, price, round(price / sum(price) over (partition by supplier_id), 3) as price_share from products;
