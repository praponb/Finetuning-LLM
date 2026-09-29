select * from (select region, round(stddev(sqft), 2) stddev_sqft, min(opened_date) min_opened_date from stores group by region) s where stddev_sqft > 21 order by stddev_sqft asc, min_opened_date asc
