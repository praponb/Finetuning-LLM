select id, cost, date_format(shipped_date, 'yyyy-MM') as shipped_date_label
from shipments
