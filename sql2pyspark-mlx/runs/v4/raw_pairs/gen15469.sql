SELECT delivered_date, LAST_DAY(shipped_date) AS shipped_date_month_end FROM shipments
