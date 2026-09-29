SELECT browser, country FROM sessions WHERE browser IS NULL INTERSECT SELECT browser, country FROM sessions WHERE country = 'US' ORDER BY country DESC;
