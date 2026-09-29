select id, launch_date, concat(category, ' - ', brand) as category_brand from products where supplier_id in (20, 35) order by id desc
