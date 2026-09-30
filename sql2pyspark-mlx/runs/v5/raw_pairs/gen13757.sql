select status, min(shipped_date) first_shipped_date from shipments where carrier in ('Flash', 'Kerry', 'DHL') and (order_id is null or cost between 29 and 94) group by status
