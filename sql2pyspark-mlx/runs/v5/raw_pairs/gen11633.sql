with ranked as (
    select
        id,
        channel,
        starts_on,
        ends_on
    from promotions
    qualify row_number() over (partition by channel order by starts_on asc, id asc) = 1
)
select
    date_trunc('quarter', ends_on) as quarter_start,
    size(collect_set(channel)) as num_channels,
    size(collect_set(channel)) as num_channels_2,
    count(*) filter (where channel = 'web') as web_rows
from ranked
group by date_trunc('quarter', ends_on)
having web_rows > 1
order by num_channels_2 desc nulls last
