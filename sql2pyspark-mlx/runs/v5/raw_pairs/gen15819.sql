select city, country, max(age) - min(age) as age_range from customers group by city, country having age_range > 21 order by age_range desc, city, country limit 10;
