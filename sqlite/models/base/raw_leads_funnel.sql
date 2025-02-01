{{ config(materialized='table') }}

SELECT * FROM {{ source('staging_db', 'leads_funnel') }}