with source as (
    select * from {{ source('medical', 'telegram_messages') }}
),

renamed as (
    select
        message_id,
        channel_name,
        cast(date as timestamp) as message_date,
        message_text,
        coalesce(views, 0) as views,
        coalesce(forwards, 0) as forwards,
        has_media,
        image_path
    from source
    -- Basic data cleaning: remove rows without a message id
    where message_id is not null
)

select * from renamed
