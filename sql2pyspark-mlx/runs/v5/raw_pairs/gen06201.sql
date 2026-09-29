select id, status, opened_date, rank() over (partition by status order by opened_date desc) rnk from accounts order by status;
