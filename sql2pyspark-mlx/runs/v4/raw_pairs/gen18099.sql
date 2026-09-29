select city, country from suppliers except select city, country from suppliers where supplier_name like 'Pac%';
