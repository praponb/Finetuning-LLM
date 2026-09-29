select city, country from suppliers where country in ('US', 'DE') union select city, country from suppliers where city = 'Chiang Mai';
