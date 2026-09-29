select id, segment, age, round(age / sum(age) over (partition by segment), 3) age_share from customers where segment = 'Corporate'
