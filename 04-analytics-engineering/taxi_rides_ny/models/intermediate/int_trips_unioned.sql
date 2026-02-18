WITH green_tripdata as (
    SELECT * FROM {{ref('stg_green_tripdata')}}
)
,
yellow_tripdata as (
    SELECT * FROM {{ref('stg_yellow_tripdata')}}
)
,
trips_unioned as (
    SELECT * FROM green_tripdata
    UNION ALL
    SELECT * FROM yellow_tripdata
)
SELECT * FROM trips_unioned