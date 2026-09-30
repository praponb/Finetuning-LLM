SELECT rating, supplier_name, country FROM suppliers WHERE country IN ('US', 'DE') OR country IN ('US', 'TH') ORDER BY country, supplier_name
