select p.id, p.product_name, p.supplier_id from products p where p.product_name not like '%Noi%' and p.supplier_id in (select id from suppliers where country in ('UK', 'TH'));
