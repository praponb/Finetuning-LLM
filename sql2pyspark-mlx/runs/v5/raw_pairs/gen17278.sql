select p.id, p.launch_date from products p where p.supplier_id not in (select id from suppliers where country in ('TH', 'UK', 'SG'));
