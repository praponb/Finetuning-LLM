select * from payments qualify row_number() over (partition by status order by amount desc nulls first, id) = 1;
