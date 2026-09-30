select country from suppliers where lower(trim(supplier_name)) = 'eurogoods gmbh' intersect select country from sessions where country in ('DE', 'UK', 'TH') order by country;
