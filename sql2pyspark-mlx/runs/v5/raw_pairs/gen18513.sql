SELECT id, units, DATE_ADD(sale_date, 86) sale_date_plus_86d FROM store_sales WHERE units > 33;
