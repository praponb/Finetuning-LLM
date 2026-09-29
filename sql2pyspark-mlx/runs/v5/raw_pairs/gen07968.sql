SELECT *, ADD_MONTHS(delivered_date, 3) delivered_date_plus_months, DATE_SUB(shipped_date, 55) shipped_date_minus_55d FROM shipments
