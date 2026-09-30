select dim.status, e.merchant, count(*) as rows_total, sum(e.amount) as total_amount from transactions e left join accounts dim on e.account_id = dim.id group by dim.status, e.merchant
