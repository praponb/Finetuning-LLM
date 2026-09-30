select id, country, age, ntile(3) over (partition by country order by age desc, id) quartile from customers where segment in ('Consumer', 'SMB', 'Corporate') order by country desc
