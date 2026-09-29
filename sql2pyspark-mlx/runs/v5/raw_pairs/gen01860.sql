select id, store_id, units, row_number() over (partition by store_id order by units desc, id) units_row from store_sales order by units
