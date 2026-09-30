select * from shipments qualify row_number() over (partition by order_id order by cost desc nulls last, id) = 1;
