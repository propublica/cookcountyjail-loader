insert into inmates (
    url,
    post_id,
    title,
    listed_date,
    price,
    location,
    city,
    state,
    description,
    registered,
    category,
    manufacturer,
    caliber,
    action,
    firearm_type,
    party,
    img,
    related_ids,
    number_of_related_listings
)
select
    url,
    post_id,
    title,
    listed_date,
    price,
    location,
    city,
    state,
    description,
    registered,
    category,
    manufacturer,
    caliber,
    action,
    firearm_type,
    party,
    img,
    related_ids,
    number_of_related_listings
from temp_listings
on conflict (url) do update
set url = excluded.url;


