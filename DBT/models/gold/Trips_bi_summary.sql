{{ config(materialized='table') }}

with cust as (
    select *,
        case when row_number() over (partition by customer_id order by dbt_valid_from) = 1
             then timestamp('1900-01-01') else dbt_valid_from end as eff_from
    from {{ ref('customers_snapshot') }}
),

drv as (
    select *,
        case when row_number() over (partition by driver_id order by dbt_valid_from) = 1
             then timestamp('1900-01-01') else dbt_valid_from end as eff_from
    from {{ ref('drivers_snapshot') }}
),

veh as (
    select *,
        case when row_number() over (partition by vehicle_id order by dbt_valid_from) = 1
             then timestamp('1900-01-01') else dbt_valid_from end as eff_from
    from {{ ref('vehicles_snapshot') }}
)

select
    t.trip_id,
    cast(t.trip_start_time as timestamp) as trip_start_time,
    cast(t.trip_end_time   as timestamp) as trip_end_time,
    date(cast(t.trip_start_time as timestamp)) as trip_date,
    t.trip_status,
    t.distance_km,
    t.fare_amount,
    t.payment_method,
    timestampdiff(MINUTE, cast(t.trip_start_time as timestamp),
                          cast(t.trip_end_time   as timestamp)) as trip_duration_min,
    t.fare_amount / nullif(t.distance_km, 0) as fare_per_km,
    c.customer_id,
    concat(c.first_name, ' ', c.last_name) as customer_name,
    c.city as customer_city,
    c.signup_date as customer_signup_date,
    d.driver_id,
    concat(d.first_name, ' ', d.last_name) as driver_name,
    d.driver_rating,
    v.vehicle_id,
    v.vehicle_type

from {{ source('source_silver', 'trips') }} t

left join cust c
  on  t.customer_id = c.customer_id
  and cast(t.trip_start_time as timestamp) >= c.eff_from
  and cast(t.trip_start_time as timestamp) <  c.dbt_valid_to

left join drv d
  on  t.driver_id = d.driver_id
  and cast(t.trip_start_time as timestamp) >= d.eff_from
  and cast(t.trip_start_time as timestamp) <  d.dbt_valid_to

left join veh v
  on  t.vehicle_id = v.vehicle_id
  and cast(t.trip_start_time as timestamp) >= v.eff_from
  and cast(t.trip_start_time as timestamp) <  v.dbt_valid_to