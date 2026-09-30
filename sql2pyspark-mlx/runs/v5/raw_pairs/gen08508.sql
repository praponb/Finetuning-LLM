select city from customers intersect select city from stores where city in ('London', 'Berlin', 'Bangkok') order by city
