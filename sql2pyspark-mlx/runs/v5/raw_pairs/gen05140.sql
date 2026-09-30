select country, case when rating >= 4.0 then 'High' when rating >= 2.2 then 'Medium' else 'Low' end as rating_tier from suppliers where not (country = 'JP' or country = 'SG');
