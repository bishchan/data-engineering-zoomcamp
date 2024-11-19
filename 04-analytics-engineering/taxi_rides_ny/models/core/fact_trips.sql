{{
    config(
        materialized='table'
    )
}}


with  cab_data as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_cab_data') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select tripid, 
    vendorid, 
    service_type,
   ratecodeid, 
  pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    pickup_datetime, 
    dropoff_datetime, 
  store_and_fwd_flag, 
 passenger_count, 
   trip_distance, 
   trip_type, 
   fare_amount, 
    extra, 
    mta_tax, 
   tip_amount, 
    tolls_amount, 
    ehail_fee, 
  improvement_surcharge, 
    total_amount, 
    payment_type, 
   payment_type_description
from cab_data
inner join dim_zones as pickup_zone
on cab_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on cab_data.dropoff_locationid = dropoff_zone.locationid