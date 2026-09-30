select id, region, count(*) over (partition by region) as region_count from stores
