<?xml version="1.0" encoding="UTF-8"?><sqlb_project><db path="weather_data.db" readonly="0" foreign_keys="1" case_sensitive_like="0" temp_store="0" wal_autocheckpoint="1000" synchronous="2"/><attached/><window><main_tabs open="structure browser pragmas query" current="3"/></window><tab_structure><column_width id="0" width="300"/><column_width id="1" width="0"/><column_width id="2" width="100"/><column_width id="3" width="3360"/><column_width id="4" width="0"/><expanded_item id="0" parent="1"/><expanded_item id="1" parent="1"/><expanded_item id="2" parent="1"/><expanded_item id="3" parent="1"/></tab_structure><tab_browse><table title="weather" custom_title="0" dock_id="4" table="4,7:mainweather"/><dock_state state="000000ff00000000fd0000000100000002000004f40000021efc0100000002fb000000160064006f0063006b00420072006f00770073006500310100000000000004f40000000000000000fb000000160064006f0063006b00420072006f00770073006500340100000000ffffffff0000011800ffffff000004f40000000000000004000000040000000800000008fc00000000"/><default_encoding codec=""/><browse_table_settings/></tab_browse><tab_sql><sql name="SQL 1*">--1. Select all data from the table
--1. Select all data from the table
SELECT * FROM weather;

--2. Get city names and their current temperature
SELECT city_name, current_temperature_2m FROM weather;

--3. Find cities where temperature is above 20°C
SELECT city_name, current_temperature_2m
FROM weather
WHERE current_temperature_2m &gt; 20;

--4. Show cities ordered by humidity descending (most humid first)
SELECT city_name, current_relative_humidity_2m
FROM weather
ORDER BY current_relative_humidity_2m DESC;

--5. Get average temperature of all cities
SELECT AVG(current_temperature_2m) AS avg_temperature FROM weather;

--6. Find city with highest wind speed
SELECT city_name, current_wind_speed_10m 
FROM weather 
ORDER BY current_wind_speed_10m DESC 
LIMIT 1;

--7. Count how many cities currently have precipitation
SELECT COUNT(*) AS cities_with_precipitation 
FROM weather 
WHERE current_precipitation &gt; 0;

</sql><current_tab id="0"/></tab_sql></sqlb_project>

