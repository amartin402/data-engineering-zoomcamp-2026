WITH taxi_zone_lookup as (
    SELECT 
        zone_id,
        borough,
        zone,
        service_zone
        FROM {{ref("taxi_zone_lookup")}}
)