SELECT country FROM suppliers EXCEPT ALL SELECT country FROM suppliers WHERE city = 'Tokyo';
