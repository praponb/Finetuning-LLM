select *, row_number() over (partition by order_id order by amount desc nulls last, id) as row_num from payments qualify row_num = 1;
