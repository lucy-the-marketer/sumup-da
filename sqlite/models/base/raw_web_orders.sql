{{ config(materialized='table') }}

SELECT * FROM {{ source('staging_db', 'web_orders') }}
