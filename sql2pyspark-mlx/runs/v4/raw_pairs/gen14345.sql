SELECT id, country, rating FROM suppliers WHERE rating < 3.6 OR supplier_name LIKE '%EuroGood%' ORDER BY id DESC, rating LIMIT 50;
