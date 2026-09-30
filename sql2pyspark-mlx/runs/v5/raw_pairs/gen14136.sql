select *, substring(country, 1, 3) as country_prefix from customers where credit_limit between 9100 and 12300
