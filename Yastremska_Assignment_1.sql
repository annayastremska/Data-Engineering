drop table if exists dataset;

--loading the json
create table dataset as
    select * from read_json("C:\Users\Lenovo\Desktop\data engineering\as1\coco_instances.json", maximum_object_size = 58454864);

-- !FIRST UNNEST: "info" column!
select json_structure(info)
from dataset; -- array

-- extracting the key-value pairs
create table info_table as
select
    json_extract(info, '$.description') as description,
    json_extract(info, '$.url') as url,
    CAST(json_extract_string(info, '$.version') as decimal) as version,
    json_extract(info, '$.year') as year,
    json_extract(info, '$.contributor') as contributor,
    strptime(json_extract_string(info, '$.date_created'), '%m/%d/%Y') as date
from dataset;

-- !SECOND UNNEST: "licenses" column!
select json_structure(licenses)
from dataset; -- object with an array

create table licenses_table as
select
    json_extract(item1, '$.url') as license_url,
    CAST(json_extract_string(item1, '$.id') as INT) as license_id,
    json_extract(item1, '$.name') as license_name
from dataset,
     UNNEST(licenses) as t(item1);

-- !THIRD UNNEST: "images" column!
select json_structure(images)
from dataset; -- object with array

create table images_table as
select
    json_extract(item2, '$.license') as image_license,
    json_extract(item2, '$.file_name') as file_name,
    CAST(json_extract_string(item2, '$.width') as INT) as width,
    CAST(json_extract_string(item2, '$.height') as INT) as height,
    CAST(json_extract_string(item2, '$.id')as INT) as image_id
from dataset,
     UNNEST(images) as t(item2);

--!FOURTH UNNEST: "annotations" column!
select json_structure(annotations)
from dataset; -- object with an array

create table idkwhatthisis_table as
select
    json_extract(item3, '$.segmentation') as segmentation,
    json_extract(item3, '$.iscrowd') as iscrowd,
    json_extract(item3, '$.bbox') as bbox
from dataset,
     UNNEST(annotations) as t(item3);

--!FIFTH UNNEST: "categories" column!
select json_structure(categories)
from dataset; -- object with an array

create table categories_table as
select
    json_extract(item4, '$.supercategory') as supercategory,
    json_extract(item4, '$.id') as category_id,
    json_extract_string(item4, '$.name') as category_name
from dataset,
     UNNEST(categories) as t(item4);


-- !!INSIGHTS!!
select * from info_table
select * from licenses_table
select * from images_table
select * from categories_table

select -- ranking insect categories by name length + identifying insects which end with "fly"
    category_name,
    LENGTH(category_name) as name_length,
    -- rank categories by name length
    DENSE_RANK() over (partition by supercategory order by LENGTH(category_name)) as dense_length_rank,
    -- identify "fly" insects
    case when category_name like '%fly' then 'Yes' else 'No' end as ends_with_fly,
from categories_table
order by dense_length_rank;

select -- check if image files are numbered sequentially and find any gaps
    file_name,
    LAG(image_id) over (order by image_id) as prev_id, -- finding previous image id
    image_id - LAG(image_id) over (order by image_id) as gap, -- gap from previous
    ROW_NUMBER() over (order by image_id) as position, -- running count
    -- marking first and last
    case
        when ROW_NUMBER() over (order by image_id) = 1 then 'First'
        when ROW_NUMBER() over (order by image_id) = count(*) over () then 'Last'
        else 'Middle'
    end as position_label
from images_table
order by image_id
limit 20;
-- no gaps found


-- !!CHECKING FOR NULLS AND DUPLICATES!!
-- info_table
select
    'info_table' as table_name,
    count(*) as total_rows,
    count(*) filter (where description is null) as nulls,
    count(*) - count(distinct description) as duplicates
from info_table;

-- licenses_table
select
    'licenses_table' as table_name,
    count(*) as total_rows,
    count(*) filter (where license_id is null) as nulls,
    count(*) - count(distinct license_id) as duplicates
from licenses_table;

-- images_table
select
    'images_table' as table_name,
    count(*) as total_rows,
    count(*) filter (where image_id is null) as nulls,
    count(*) - count(distinct image_id) as duplicates
from images_table;

-- categories_table
select
    'categories_table' as table_name,
    count(*) as total_rows,
    count(*) filter (where category_id is null) as nulls,
    count(*) - count(distinct category_id) as duplicates
from categories_table;