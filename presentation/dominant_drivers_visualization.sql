-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name, count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points,
rank() OVER (ORDER BY avg(calculated_points) DESC) as rank
FROM presentation.race_results_calculated 
GROUP BY driver_name
HAVING count(*)>50
ORDER BY average_points DESC


-- COMMAND ----------

SELECT race_year, driver_name,
count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points
FROM presentation.race_results_calculated
WHERE driver_name IN (SELECT driver_name
                      FROM v_dominant_drivers
                      WHERE rank<=10)
GROUP BY race_year, driver_name
ORDER BY race_year, average_points DESC

-- COMMAND ----------

SELECT driver_name,
count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points
FROM presentation.race_results_calculated
WHERE driver_name IN (SELECT driver_name
                      FROM v_dominant_drivers
                      WHERE rank<=10)
GROUP BY driver_name
ORDER BY average_points DESC
