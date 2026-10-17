with hours as (
    select
        dateadd(
            hour,
            row_number() over (order by seq4()) - 1,      
            '2023-09-14 18:30:00'::timestamp_ntz
        ) as hour_start_utc
    from table(generator(rowcount => 40000))               
),
spine as (
    select hour_start_utc
    from hours
        where hour_start_utc <= dateadd(hour, 48, sysdate())
),
enriched as (
    select
        hour_start_utc,
        dateadd(hour, 1, hour_start_utc) as hour_end_utc,
        convert_timezone('UTC', 'Asia/Kolkata', hour_start_utc)::timestamp_ntz  as hour_start_ist,
        to_date(convert_timezone('UTC', 'Asia/Kolkata', hour_start_utc)) as date_ist,
        hour(convert_timezone('UTC', 'Asia/Kolkata', hour_start_utc)) as hour_of_day_ist,
        dayname(convert_timezone('UTC', 'Asia/Kolkata', hour_start_utc)) as day_name_ist
    from spine
)

select
    hour_start_utc,
    hour_end_utc,
    hour_start_ist,
    date_ist,
    hour_of_day_ist,
    day_name_ist,
    year(date_ist)  as year_ist,
    month(date_ist) as month_ist,
    (date_ist between date_from_parts(year(date_ist), 9, 15) and date_from_parts(year(date_ist), 12, 15))   as is_burning_season,
    case
        when date_ist between date_from_parts(year(date_ist), 9, 15) and date_from_parts(year(date_ist), 12, 15)
        then year(date_ist)
    end  as season,
    (date_ist between date_from_parts(year(date_ist), 10, 15) and date_from_parts(year(date_ist), 11, 30)) as is_peak_burning,
    (hour_of_day_ist >= 18 or hour_of_day_ist < 6)   as is_night_ist
from enriched