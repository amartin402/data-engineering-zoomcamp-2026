SELECT 
--identifiers
vendorid as vendor_id,
ratecodeid as rate_code_id,
pulocationid as pickup_location_id,
dolocationid as dropoff_location_id,

--timestamps
cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

-- trip info
store_and_fwd_flag,
cast(passenger_count as int) as passenger_count,
trip_distance,
cast(trip_type as int) as trip_type,

-- payment info
cast(fare_amount as numeric) as fare_amount,
cast(extra as numeric) as extra,
cast(mta_tax as numeric) as mta_tax,
cast(tip_amount as numeric) as tip_amount,
cast(tolls_amount as numeric) as tolls_amount,
cast(improvement_surcharge as numeric) as improvement_surcharge,
cast(ehail_fee as numeric) as ehail_fee,
cast(total_amount as numeric) as total_amount,
cast(payment_type as numeric) as payment_type
FROM {{source("raw_data", "green_tripdata")}} 
WHERE vendorid is not null