select
	segment_events.visitor_vso_id
	, max(segment_events.event_created_at) as last_signed_in_ecomm_website
    , count(distinct case when dates.is_last_12_complete_months then segment_events.tracks_message_id end) as total_logins_last_12_months_ecomm_website
from {{ ref('segment__events') }} as segment_events
left join {{ ref('dates') }} on segment_events.event_created_at::date = dates.date
where
    event_name = 'Signed In'
    and visitor_vso_id is not null
group by 1
