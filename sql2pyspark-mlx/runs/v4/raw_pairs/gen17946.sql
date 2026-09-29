select country, city from suppliers intersect all select country, city from suppliers where rating <= 2.1;
