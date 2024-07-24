-- Databricks notebook source
SELECT team as team_name, 
count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points 
FROM presentation.race_results_calculated 
GROUP BY team
HAVING count(*)>100
ORDER BY average_points DESC


-- COMMAND ----------

SELECT team as team_name, 
count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points 
FROM presentation.race_results_calculated 
WHERE race_year BETWEEN 2010 AND 2020
GROUP BY team
HAVING count(*)>100
ORDER BY average_points DESC
