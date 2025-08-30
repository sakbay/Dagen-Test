{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT 
        date_day
    FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31', INTERVAL 1 DAY)) AS date_day
),

date_details AS (
    SELECT
        date_day,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
        EXTRACT(DAYOFYEAR FROM date_day) AS day_of_year,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        
        -- Month name
        FORMAT_DATE('%B', date_day) AS month_name,
        FORMAT_DATE('%b', date_day) AS month_name_short,
        
        -- Day name
        FORMAT_DATE('%A', date_day) AS day_name,
        FORMAT_DATE('%a', date_day) AS day_name_short,
        
        -- Quarter label
        CONCAT('Q', EXTRACT(QUARTER FROM date_day), ' ', EXTRACT(YEAR FROM date_day)) AS quarter_label,
        
        -- Year-Month
        FORMAT_DATE('%Y-%m', date_day) AS year_month,
        
        -- Business day flags
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN FALSE  -- Sunday = 1, Saturday = 7
            ELSE TRUE 
        END AS is_weekday,
        
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE
            ELSE FALSE 
        END AS is_weekend,
        
        -- First and last day flags
        CASE 
            WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE
            ELSE FALSE
        END AS is_first_day_of_month,
        
        CASE 
            WHEN date_day = LAST_DAY(date_day) THEN TRUE
            ELSE FALSE
        END AS is_last_day_of_month,
        
        -- Current date flags
        CASE 
            WHEN date_day = CURRENT_DATE() THEN TRUE
            ELSE FALSE
        END AS is_today,
        
        CASE 
            WHEN date_day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN TRUE
            ELSE FALSE
        END AS is_yesterday
        
    FROM date_spine
)

SELECT * FROM date_details