with detections as (
    select * from {{ ref('stg_image_detections') }}
),

messages as (
    select * from {{ ref('fct_messages') }}
),

final as (
    select
        d.message_id,
        m.channel_key,
        m.date_key,
        d.image_category,
        d.detected_objects,
        d.confidence_score
    from detections d
    inner join messages m on d.message_id = m.message_id
)

select * from final
