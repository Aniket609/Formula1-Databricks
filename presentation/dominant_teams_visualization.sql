-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team as team_name, 
count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points,
rank() over (ORDER BY avg(calculated_points) DESC) as rank
FROM presentation.race_results_calculated 
GROUP BY team
HAVING COUNT(*)>100
ORDER BY average_points DESC


-- COMMAND ----------

SELECT race_year, team as team_name, 
count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points
FROM presentation.race_results_calculated
WHERE team IN (SELECT team_name FROM v_dominant_teams WHERE rank<=5) 
GROUP BY race_year,team
ORDER BY average_points DESC


-- COMMAND ----------

SELECT race_year, team as team_name, 
count(*) as total_races,
sum(calculated_points) as total_points, 
avg(calculated_points) as average_points
FROM presentation.race_results_calculated
WHERE team IN (SELECT team_name FROM v_dominant_teams WHERE rank<=5) 
GROUP BY race_year,team
ORDER BY average_points DESC

