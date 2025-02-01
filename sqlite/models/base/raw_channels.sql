{{ config(materialized='table') }}

SELECT * FROM {{ source('staging_db', 'channels') }}