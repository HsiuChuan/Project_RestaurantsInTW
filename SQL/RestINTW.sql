USE google_stores;
grant all privileges on *.* to root@localhost identified by '' with grant option;

USE google_stores;
CREATE TABLE store_info(
	name text ,
	place_id text,
	lat DECIMAL(10,7) NOT NULL DEFAULT 0,
	lng DECIMAL(10,7) NOT NULL DEFAULT 0,
	price_level integer NOT NULL DEFAULT 0,
	rating DECIMAL(2,1) NOT NULL DEFAULT 0,
	user_ratings_total integer NOT NULL DEFAULT 0,
	related_place text,
	key1 text,
	key2 text,
	key3 text
)CHARACTER SET utf8mb4;


CREATE TABLE update_info(
	place_id text,
	rating DECIMAL(2,1) NOT NULL DEFAULT 0,
	user_ratings_total integer NOT NULL DEFAULT 0,
    url text,
    website text,
	formatted_phone_number text,
	formatted_address text,
    Update_time DATE
)CHARACTER SET utf8mb4;




-- Trigger
/*
1. Update 
Once data insered into table update_info,
store_info.rating & store_info.user_ratings_total should change immediately.

2. Add new Columns
Add new columns into store_info from update_info left, 
and insert the values with Trigger.
*/
-- 
-- Add new columns into store_info
ALTER TABLE store_info
ADD COLUMN url text AFTER user_ratings_total,
ADD COLUMN website text AFTER url,
ADD COLUMN formatted_phone_number text AFTER website,
ADD COLUMN formatted_address text AFTER formatted_phone_number,
ADD COLUMN Update_time text AFTER formatted_address;

-- DROP TRIGGER score_ud;
CREATE TRIGGER score_ud
AFTER INSERT ON update_info
FOR EACH ROW 
UPDATE store_info
SET store_info.rating = NEW.rating,
	store_info.user_ratings_total = NEW.user_ratings_total,
	store_info.url = NEW.url,
	store_info.website = NEW.website,
	store_info.formatted_phone_number = NEW.formatted_phone_number,
	store_info.formatted_address = NEW.formatted_address,
	store_info.Update_time = NEW.Update_time
WHERE store_info.place_id = NEW.place_id;


