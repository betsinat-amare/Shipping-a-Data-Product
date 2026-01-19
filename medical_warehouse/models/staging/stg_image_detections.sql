with source as (
    select * from {{ source('medical', 'image_detections') }}
),

renamed as (
    select
        message_id,
        image_path,
        -- Parse the stringified list back if needed, or keep as text
        -- For simplicity in this raw->stg pass, we keep it as is, 
        -- or could cast to array if using Postgres native arrays.
        -- Assuming it stored as a string representation of list like "['bottle', 'person']"
        detected_objects, 
        cast(confidence_score as numeric) as confidence_score,
        image_category
    from source
)

select * from renamed
