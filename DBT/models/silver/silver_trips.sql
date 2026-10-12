{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trip_id'
) }}


{% set cols = [
    'trip_id',
    'driver_id',
    'customer_id',
    'vehicle_id',
    'trip_start_time',
    'trip_end_time',
    'distance_km',
    'trip_status',
    'last_updated_timestamp',
    '_ingested_at'
] %}

select
{% for c in cols %}
     t.{{ c }}
     {% if not loop.last %},
     {% endif %}
{% endfor %}
from {{ source('source_bronze', 'trips') }} t

{% if is_incremental() %}
where t._ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}