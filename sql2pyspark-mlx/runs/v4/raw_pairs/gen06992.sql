select carrier, round((100 * sum(case when carrier = 'Flash' then 1 else 0 end)) / count(*), 1) as pct_flash, sum(case when carrier = 'DHL' then 1 else 0 end) as dhl_count
from shipments
where carrier in ('UPS', 'DHL') or cost <= 89
group by carrier
order by dhl_count;
