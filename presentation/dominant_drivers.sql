-- Databricks notebook source
SELECT driver_name, count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points 
FROM presentation.race_results_calculated 
GROUP BY driver_name
HAVING count(*)>50
ORDER BY average_points DESC


-- COMMAND ----------

SELECT driver_name, count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points 
FROM presentation.race_results_calculated 
WHERE race_year BETWEEN 2010 AND 2020
GROUP BY driver_name
HAVING count(*)>50
ORDER BY average_points DESC
