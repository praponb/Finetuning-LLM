SELECT id, country FROM suppliers WHERE rating >= 3.3 INTERSECT SELECT id, country FROM suppliers WHERE country = 'US';
