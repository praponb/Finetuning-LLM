SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY delivered_date, id) AS dedup_rank
FROM shipments
QUALIFY dedup_rank = 1
