select * from store_sales qualify row_number() over (partition by store_id order by units nulls last, id) = 1;
