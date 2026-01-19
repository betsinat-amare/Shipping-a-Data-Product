-- Ensure that view counts are never negative
select *
from {{ ref('stg_telegram_messages') }}
where views < 0
