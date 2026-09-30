select city from customers except select city from stores where opened_date between '2025-03-01' and '2025-04-01' order by city desc;
