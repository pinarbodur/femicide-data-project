{{ config(materialized='table') }}

SELECT informant_of_data
,historical_date
,month_of_femicide
FROM {{ source("base", "femicide_2023")}}
WHERE informant_of_data is not null
AND historical_date is not null