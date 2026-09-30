select id, carrier, delivered_date, rank() over (partition by carrier order by delivered_date desc) as rnk from shipments;
