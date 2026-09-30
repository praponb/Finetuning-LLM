select category, id from products intersect select category, id from products where brand = 'Umbrella';
