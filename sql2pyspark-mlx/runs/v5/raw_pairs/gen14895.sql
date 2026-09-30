select id, supplier_name, rating, coalesce(supplier_name, 'N/A') supplier_name_filled, ceil(rating) rating_whole from suppliers order by id desc;
