select page, concat_ws(', ', sort_array(collect_set(event_type))) as event_type_values from web_events group by page order by page
