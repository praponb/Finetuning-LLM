SELECT country FROM suppliers WHERE country != 'US' INTERSECT SELECT country FROM sessions WHERE device != 'mobile' ORDER BY country
