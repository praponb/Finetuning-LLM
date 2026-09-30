select id, shipped_date, percent_rank() over (order by shipped_date desc) as shipped_date_pct_rank from shipments
