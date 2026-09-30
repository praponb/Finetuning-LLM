SELECT city, country FROM suppliers EXCEPT DISTINCT SELECT city, country FROM suppliers WHERE country <> 'SG'
