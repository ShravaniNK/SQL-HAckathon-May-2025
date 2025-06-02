--------------------------------------TEAM-2_SQL_HACKATHON_TheQueryQueens_MAY2025------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 1. Display the duplicate participant_id and total number of duplicate records, 
      if present in any table. Delete the duplicate records from that table. */
------------------------------------------------------------------------------------------------------------------------------------ 
-- Only the body_compositions table has 1 duplicate row for each participant_id
-- 600 records are duplicate out of total 1200
SELECT participant_id, COUNT(*) AS duplicate_count
FROM body_compositions 
GROUP BY participant_id
HAVING COUNT(*) > 1;

-- To delete the duplicate record, use the internal unique row ID called 'ctid'  
DELETE FROM body_compositions
WHERE ctid NOT IN (
	SELECT MAX(ctid)
	FROM body_compositions
	GROUP BY  participant_id, weight_v1, weight_v3, 
	weight_change_percent, abdominal_circumference_v3 );

-- check the table now
SELECT  * FROM body_compositions;

------------------------------------------------------------------------------------------------------------------------------------
/* Qn. 2. Create a view and extract the calcium trend between the visits on gdm patients. */
------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW calcium_trends_by_gdm AS
SELECT 
    B.participant_id, 
    B.calcium_v1, 
    B.calcium_v3,
    ROUND((100 * (B.calcium_v3 - B.calcium_v1) / B.calcium_v1)::numeric,2) AS calcium_change_percent
FROM 
    biomarkers B
INNER JOIN 
    glucose_tests GT 
    ON B.participant_id = GT.participant_id
WHERE 
    GT.diagnosed_gdm = 1
    AND B.calcium_v1 IS NOT NULL
    AND B.calcium_v3 IS NOT NULL;

--check the view
SELECT * FROM Calcium_Trends_By_GDM ORDER BY 4;
--There were 42 patients diagnosed with gdm and had calcium levels tested.
--The calcium levels increased from visit 1 to 3 for most of the patients  
--except for 8 patients, there was a drop in the level and for 2 patients there was no change.

------------------------------------------------------------------------------------------------------------------------------------
/* Qn. 3. Provide the number of patients aggregated by year based on forms they signed. */
------------------------------------------------------------------------------------------------------------------------------------
SELECT COUNT(participant_id) AS Number_Of_Participants_By_Year, 
	EXTRACT(YEAR FROM date_form_signed) AS Year_Signed
FROM documentation_track
GROUP BY EXTRACT(YEAR FROM date_form_signed);

------------------------------------------------------------------------------------------------------------------------------------
/* Qn. 4. Calculate % of gdm diagnosed patients whose age is above 30. */
------------------------------------------------------------------------------------------------------------------------------------
SELECT 
    ROUND(100.0 * COUNT(*) FILTER (
        WHERE D.age_above_30 = 1 AND GT.diagnosed_gdm = 1
    ) / COUNT(*), 2) AS Percentage_Of_Patients_With_GDM
FROM demographics D
JOIN  glucose_tests GT on D.participant_id = GT.participant_id;

-------------------------------------------------------------------------------------------------------------------------------------------
/* Qn. 5. Create a trigger on the Demographics table that monitors and logs all INSERT, UPDATE, and DELETE operations performed 
on the table.*/
-------------------------------------------------------------------------------------------------------------------------------------------
-- Create Operation Enum 
-- ENUMs are custom data types that define a fixed, ordered set of values, useful for simplifying queries
CREATE TYPE operation_enum AS ENUM('Insert', 'Update', 'Delete')

-- STEP 1. Create an demographics Audit Table on which the trigger should be executed/fired: 
CREATE TABLE IF NOT EXISTS demographics_audit_table (
    log_id SERIAL PRIMARY KEY,
	participant_id INT,
    ethnicity TEXT,
    age_above_30 INT,
	height_m  DECIMAL(10,2),
	bmi_kgm2_v1  DECIMAL(10,2),
    operation  operation_enum not null,
    changed_date timestamp  DEFAULT CURRENT_TIMESTAMP
);

--DROP TABLE demographics_audit_table;

--check the newly created audit table
SELECT * FROM demographics_audit_table;

-- DROP TABLE demographics_audit_table;

-- STEP 2.  Create/Define a user-defined function called "demographicsauditlog_function()" for the Trigger 
--          to execute whenever an DML operation(INSERT, UPDATE, or DELETE) occurs on the demographics table.

-- To create a new trigger, you define a trigger function first, and then bind this trigger function to a table.
CREATE OR REPLACE FUNCTION demographicsauditlog_function()
Returns trigger AS $$
BEGIN 
	IF (TG_OP = 'INSERT') THEN
		INSERT INTO demographics_audit_table(
		participant_id, 
		ethnicity, 
		age_above_30, 
		height_m,
		bmi_kgm2_v1,  
		operation,
		changed_date
		)
        VALUES ( 
		NEW.participant_id, 
		NEW.ethnicity, 
		NEW.height_m,
		NEW.age_above_30, 
		NEW.bmi_kgm2_v1, 
		'Insert',
		CURRENT_TIMESTAMP
		);
	
	ELSEIF (TG_OP = 'UPDATE') THEN
		INSERT INTO demographics_audit_table(
		participant_id, 
		ethnicity, 
		age_above_30, 
		height_m,
		bmi_kgm2_v1,  
		operation,
		changed_date
		)
        VALUES ( 
		OLD.participant_id, 
		OLD.ethnicity, 
		OLD.height_m,
		OLD.age_above_30, 
		NEW.bmi_kgm2_v1, 
		'Update',
		CURRENT_TIMESTAMP
		);

	ELSEIF (TG_OP = 'DELETE') THEN
		INSERT INTO demographics_audit_table(
		participant_id, 
		ethnicity, 
		age_above_30, 
		height_m,
		bmi_kgm2_v1,  
		operation,
		changed_date
		)
        VALUES ( 
		OLD.participant_id, 
		OLD.ethnicity, 
		OLD.height_m,
		OLD.age_above_30, 
		OLD.bmi_kgm2_v1, 
		'Delete',
		CURRENT_TIMESTAMP
		);

	END IF;
RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- STEP 3. Create and bind the Trigger that calls the function when changes occur in the film table.
CREATE TRIGGER demographics_audit_trigger
AFTER INSERT OR UPDATE OR DELETE
ON demographics
FOR EACH ROW
EXECUTE FUNCTION demographicsauditlog_function();

--STEP 4. Let's INSERT sample data into demographics table 
INSERT INTO demographics(participant_id, ethnicity, age_above_30, height_m, bmi_kgm2_v1)
VALUES(601,'Asian', 1, 1.75, 24.6);

INSERT INTO demographics(participant_id, ethnicity, age_above_30, height_m, bmi_kgm2_v1)
VALUES(602, 'Mixed',0,  1.60, 27.4);

-- Check/ Verify the entry inthe film table
SELECT * FROM demographics ORDER BY 1 DESC;

--check in the audit table
SELECT * FROM demographics_audit_table;

--STEP 5. Let us UPDATE the bmi for participant_id = 602 to 28.94
UPDATE demographics SET bmi_kgm2_v1 = 28.94 WHERE participant_id = 602;

--Check/ Verify the entry inthe film table
SELECT * FROM demographics ORDER BY 1 DESC;

--check in the audit table
SELECT * FROM demographics_audit_table;

-- STEP 6. Lets DELETE a row from the demographics table
DELETE FROM demographics WHERE participant_id = 601;
DELETE FROM demographics WHERE participant_id = 602;

--Check/ Verify the entry inthe film table
SELECT * FROM demographics ORDER BY 1 DESC;

--check in the audit table
SELECT * FROM demographics_audit_table;

--DROP TRIGGER demographics_audit_trigger ON demographics;
--DROP FUNCTION demographicsauditlog_function();
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 6. There is a requirement to extract a list of GDM patients.Since this is very frequent extraction, can you provide a 
  solution to store this data for frequent usage by avoiding repetitive use of database resources .
  Hint:No new Table creation required.*/
---------------------------------------------------------------------------------------------------------------------------------
--Solution: Materialized views store the results of a query on a disk
--          as a physical table, and there is no recomputing required, thus 
--          saving processing time and database resources for frequently run queries.
CREATE MATERIALIZED VIEW gdm_patients_list AS
SELECT 
    participant_id
FROM glucose_tests
WHERE diagnosed_gdm = 1;

-- To refresh the materialized view
REFRESH MATERIALIZED VIEW gdm_patients_list;

--Explain Analyze
SELECT * FROM gdm_patients_list;


--DROP MATERIALIZED VIEW gdm_patients_list;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 7. Display progression/Remission of diabetes using HbA1c values in gdm patients.*/
---------------------------------------------------------------------------------------------------------------------------------
-- Comparing the values of visit 2 & 3 with the previous visit 1 to categorize as progression or remission.
-- When all 3 the values of hba1c are increasing in order, then 'Progression'.
-- When all the 3 the values of hba1c are decreasing in order, then 'Regression'.
-- When v2>v1, and then v3< v1, but still v3> v1, then it is also 'Progression'.
-- When v2<v1, and then v3>v1, but still v3<v1, then it is also 'Regression'.
-- When there is no change in the values, then it is STABLE.
-- When there is only value given and remaining are NULL, then 'INSUFFICIENT DATA'.

CREATE OR REPLACE VIEW hba1c_progression_status AS
SELECT 
    participant_id, 
    hba1c_v1, 
    hba1c_v2, 
    hba1c_v3,
    CASE
        WHEN hba1c_v2 IS NULL AND hba1c_v3 IS NULL THEN 'INSUFFICIENT DATA'
        
        WHEN hba1c_v3 IS NOT NULL AND (
                 hba1c_v3 > COALESCE(hba1c_v1, hba1c_v3)
              OR hba1c_v3 > COALESCE(hba1c_v2, hba1c_v3)
             ) THEN 'Progression'
        
        WHEN hba1c_v3 IS NOT NULL AND (
                 hba1c_v3 < COALESCE(hba1c_v1, hba1c_v3)
              AND (hba1c_v2 IS NULL OR hba1c_v3 < hba1c_v2)
             ) THEN 'Regression'
        
        WHEN hba1c_v3 IS NULL AND hba1c_v2 IS NOT NULL AND hba1c_v2 > hba1c_v1 THEN 'Progression'
        WHEN hba1c_v3 IS NULL AND hba1c_v2 IS NOT NULL AND hba1c_v2 < hba1c_v1 THEN 'Regression'
        
        ELSE 'STABLE'
    END AS status
FROM glucose_tests
WHERE diagnosed_gdm = 1;

--Check the view
SELECT * FROM hba1c_progression_status;

--Summary of Count of patients by status category
SELECT status,
    COUNT(*) AS participant_count
FROM hba1c_progression_status
GROUP BY status
ORDER BY participant_count;

--The status column shows the progression or regression of the hba1c values for all the 3 visits. 
--3 participants have only the first visit data, so it is not sufficient to compare.
--7 participants have stable values for the visits .
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 8. Calculate New Gestational Age as number of days column  using gestational_age_v1
--   Hint:(Gestational age is mentioned in Weeks+days).*/
---------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW new_gestational_age AS
SELECT
	participant_id, 
    gestational_age_v1,
    COALESCE(split_part(gestational_age_v1, '+', 1)::int, 0) * 7 +
    COALESCE(NULLIF(split_part(gestational_age_v1, '+', 2), '')::int, 0) AS gestational_days
FROM pregnancy_info;

SELECT * FROM new_gestational_age;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 9. Display participants with a significant increase (greater than 20%) in both 
--creatinine and urine albumin levels between visit 1 and visit 3.*/
---------------------------------------------------------------------------------------------------------------------------------
SELECT 
	participant_id, creatinine_change_percent,
	ROUND((100 * ("U Albumin_V3" - "U Albumin_V1") / "U Albumin_V1")::numeric,2) 
	AS U_albumin_change_percent
FROM kidney_function 
WHERE creatinine_change_percent > 20 
AND 
ROUND((100 * ("U Albumin_V3" - "U Albumin_V1") / "U Albumin_V1")::numeric,2) > 20
ORDER BY 1;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 10. Select only odd rows from demographics table using Windows function and derived subquery.*/
---------------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (ORDER BY participant_id) AS odd_row_number
  FROM demographics
) AS derived
WHERE odd_row_number % 2 = 1;

---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 11. Create a table where a column automatically populates with decrementing values. 
Demonstrate how the values decrease over time.*/
---------------------------------------------------------------------------------------------------------------------------------
--DROP TABLE decrementing_table; 

--Create a table
CREATE TABLE decrementing_table (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    value INTEGER
);

-- Insert an initial value of 50 into the table
INSERT INTO decrementing_table (value)
SELECT 50;

--check the table
SELECT * FROM decrementing_table;

-- Initiate decrementing process,
-- for each insert, the value gets reduced by the specified number
INSERT INTO decrementing_table (value)
SELECT value - 2 FROM decrementing_table ORDER BY id DESC LIMIT 1;

INSERT INTO decrementing_table (value)
SELECT value - 4 FROM decrementing_table  ORDER BY id DESC LIMIT 1;

INSERT INTO decrementing_table (value)
SELECT value - 5 FROM decrementing_table ORDER BY id DESC LIMIT 1;


--check the table now  and also display the change value
SELECT id, timestamp, value,
	       value - LAG(value) OVER (ORDER BY timestamp) AS reduced_by
FROM decrementing_table
ORDER BY timestamp;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 12. Create materialized view, calculate and categorize MAP for all participants. 
Display the ranking and analyze the distribution across MAP categories.*/
---------------------------------------------------------------------------------------------------------------------------------
-- MAP = (2*DBP + SBP)/3
-- MAP < 70 , then 'Hypotension'
-- MAP > 100 , then 'Hypertension'
-- MAP >= 70 AND MAP <= 100, then 'Normal'
-- DROP MATERIALIZED VIEW participant_MAP_analysis;

CREATE MATERIALIZED VIEW participant_MAP_analysis AS
SELECT 
    participant_id,
	--calculate the MAP of visit 1
	ROUND(((2*diastolic_bp_v1 + systolic_bp_v1)/3),2) AS MAP_v1, 
	--calculate the MAP of visit 3
	ROUND(((2*diastolic_bp_v3 + systolic_bp_v3)/3),2) AS MAP_v3,
	-- categorize MAP of visit 1 values into low, normal & high
	CASE
		WHEN ((2*diastolic_bp_v1 + systolic_bp_v1)/3) < 70 THEN 'LOW'
		WHEN ((2*diastolic_bp_v1 + systolic_bp_v1)/3) BETWEEN 70 AND 100 THEN 'NORMAL'
		ELSE 'HIGH'
	END AS map_cateogorize_v1,
	-- categorize MAP of visit 3 values into low, normal & high
	CASE
		WHEN ((2*diastolic_bp_v3 + systolic_bp_v3)/3) < 70 THEN 'LOW'
		WHEN ((2*diastolic_bp_v3 + systolic_bp_v3)/3) BETWEEN 70 AND 100 THEN 'NORMAL'
		ELSE 'HIGH'
	END AS map_cateogorize_v3,
	
	--calculate the average of visit 1 and 3
	ROUND(((2*diastolic_bp_v1 + systolic_bp_v1) / 3.0 + (2*diastolic_bp_v3 + systolic_bp_v3) / 3.0) / 2.0, 2) AS MAP_AVG,
	--Rank the average map values of visit 1 & 3 using dense rank
   DENSE_RANK() OVER(
   ORDER BY (((2*diastolic_bp_v1 + systolic_bp_v1) / 3 + (2*diastolic_bp_v3 + systolic_bp_v3) / 3) / 2) DESC) 
   AS map_rank
FROM vital_signs
WHERE    diastolic_bp_v1 IS NOT NULL
	 AND diastolic_bp_v3 IS NOT NULL
	 AND systolic_bp_v1 IS NOT NULL
	 AND systolic_bp_v3 IS NOT NULL;

SELECT * FROM participant_MAP_analysis;

--Analyze the distribution across MAP categories for visit 1
SELECT
	map_cateogorize_v1,
    COUNT(*) AS patient_count
FROM participant_MAP_analysis
GROUP BY map_cateogorize_v1;

--Analyze the distribution across MAP categories for visit 3
SELECT
    map_cateogorize_v3,
    COUNT(*) AS patient_count
FROM participant_MAP_analysis
GROUP BY map_cateogorize_v3;

--Analyze the change in  MAP categories for visit 1 & 3
SELECT
	map_cateogorize_v1,
    map_cateogorize_v3,
    COUNT(*) AS patient_count
FROM participant_MAP_analysis
GROUP BY map_cateogorize_v1, map_cateogorize_v3;

---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 13. How many participants were admitted in the present month of any year.*/
---------------------------------------------------------------------------------------------------------------------------------
-- Assuming the date form signed as the day the participants were admitted
SELECT 
  EXTRACT(YEAR FROM date_form_signed) AS year,
  EXTRACT(MONTH FROM date_form_signed) AS month,
  COUNT(*) AS participants_count
FROM documentation_track
WHERE EXTRACT(YEAR FROM date_form_signed) = 2015
GROUP BY year, month;

---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 14. Generate random age  between 18 and 50 for all participants. 
Calculate birth year for all participants using stored procedure. */
---------------------------------------------------------------------------------------------------------------------------------
-- DROP PROCEDURE generate_participant_age_and_birth_year();
CREATE OR REPLACE PROCEDURE generate_participant_age_and_birth_year()
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE demographics
  SET age = CASE
              WHEN age_above_30 = 0 THEN FLOOR(random() * (30 - 18 + 1) + 18)::int
              WHEN age_above_30 = 1 THEN FLOOR(random() * (50 - 31 + 1) + 31)::int
            END;
--Now calculate birthyear with the generated age
  UPDATE demographics
  SET birth_year = EXTRACT(YEAR FROM CURRENT_DATE)::int - age;
END;
$$;

-- call the procedure
CALL generate_participant_age_and_birth_year();

-- check the table now
SELECT participant_id, age_above_30, age,birth_year FROM demographics;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 15. Show the GDM prevalence by race.*/
---------------------------------------------------------------------------------------------------------------------------------
--select  * from demographics order by participant_id;
--select  * from glucose_tests order by participant_id;
--select count(*) from glucose_tests where diagnosed_gdm = 1; output:74
--select count(*), ethnicity from demographics group by ethnicity order by 2; 
SELECT 
    D.ethnicity, COUNT(*) AS GDM_Prevelance_By_Race
FROM demographics D
JOIN  glucose_tests GT on D.participant_id = GT.participant_id
WHERE GT.diagnosed_gdm = 1
GROUP BY D.ethnicity;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 16. Query to show total number of columns from all tables in database along with data types for each table.*/
---------------------------------------------------------------------------------------------------------------------------------
SELECT table_name, COUNT(*) AS total_columns,
	'{' || STRING_AGG(column_name || ': ' || data_type, ', ' ORDER BY ordinal_position) || '}' 
	AS columns_with_datatypes
FROM information_schema.columns
WHERE table_schema = 'public' AND  
      table_name IN (SELECT table_name FROM information_schema.tables)
GROUP BY table_name
ORDER BY total_columns;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 17. "Generate a new attribute ""High risk pregnancy"" to demographics table. 
Create a trigger that automatically populates the existing records.
Hint: Consider age,smoking,high risk ." */
---------------------------------------------------------------------------------------------------------------------------------
-- Add the new column high_risk_pregnancy
ALTER TABLE demographics
ADD COLUMN high_risk_pregnancy BOOLEAN;

-- Create the trigger function
CREATE OR REPLACE FUNCTION populate_high_risk_pregnancy()
RETURNS TRIGGER 
AS $$
BEGIN
  NEW.high_risk_pregnancy := 
    CASE 
      WHEN NEW.age_above_30 = 1 OR NEW.smoking IN ('current', 'ex') OR NEW.highrisk = 1 
      THEN TRUE 
      ELSE FALSE 
    END;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- Create the trigger
CREATE TRIGGER trg_populate_high_risk_pregnancy
BEFORE INSERT OR UPDATE ON demographics
FOR EACH ROW
EXECUTE FUNCTION populate_high_risk_pregnancy();

-- To populate the existing records for the new attribute- high_risk_pregnancy, updating any column value
-- ( ethnicity)
-- with the same value as before, so that the trigger is fired and executed.

UPDATE demographics
SET ethnicity = ethnicity;

-- Check if the new column is populated
SELECT participant_id, ethnicity, age_above_30, smoking, alcohol_intake, highrisk, high_risk_pregnancy  
FROM demographics;
---------------------------------------------------------------------------------------------------------------------------------
/* Q. 18. Write a query to find the number of Participants by expected date of delivery month 
(Display with month name only)*/
---------------------------------------------------------------------------------------------------------------------------------
SELECT 
COUNT(participant_id) AS total_participants,
to_char(to_date(EXTRACT(MONTH from edd_v1)::text, 'MM'), 'Month') as expected_date_of_delivery_month_name
FROM pregnancy_info
GROUP BY EXTRACT(MONTH from edd_v1)
;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 19. Create a view without using any schema or table and check the created view using a select statement. */
---------------------------------------------------------------------------------------------------------------------------------
CREATE VIEW vw_sql_hackathon
AS
SELECT 
'The Query Queens' AS team_name,
2 AS team_number,
'Shruti, Shravani, Simantini, Savitha, Meenaa' AS team_member_list
;

SELECT * FROM vw_sql_hackathon;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn 20. Find the count of patients who have both gestational diabetes and anemia. */
---------------------------------------------------------------------------------------------------------------------------------
--Referred Links:
--https://pmc.ncbi.nlm.nih.gov/articles/PMC5558393/#:~:text=The%20major%20signs%20and%20symptoms,mucosal%20paleness%2C%20and%20angular%20stomatitis.
--https://www.healthpartners.com/blog/pregnancy-appointments-timeline/

--https://www.ncbi.nlm.nih.gov/books/NBK557783/#:~:text=Anemia%20in%20Pregnancy%20*%20First%20trimester%20%E2%80%93,%E2%80%93%20Hemoglobin%20level%20%3C10.5%20to%2011%20g/dL.

-- https://www.healthpartners.com/blog/pregnancy-appointments-timeline/

-- https://pmc.ncbi.nlm.nih.gov/articles/PMC11002965/

-- Assumptions based on the referred links: 
--As per studies, the lower threshold value for hemoglobin (Hb) in pregnant women is <11 g/dL --during the 1st and 3rd trimesters, and <10.5 g/dL during the 2nd trimester.

--Visit 1 happens in the first trimester and visit 3 happens in the second trimester.
---------------------------------------------------------------------------------------------------------------------------------
WITH anemia_category AS (
SELECT 
bm.participant_id,
CASE    
	WHEN  (bm.hb_v1 IS NOT NULL AND bm.hb_v1 < 11.0)
       OR (bm.hb_v3 IS NOT NULL AND bm.hb_v3 < 10.5)
    THEN 'Yes'
    ELSE 'No'
  END AS is_anemic
FROM biomarkers bm 
) 
SELECT
COUNT(*) AS participants_with_gestational_diabetes_and_anemia
FROM anemia_category ac
INNER JOIN glucose_tests gt
ON ac.participant_id = gt.participant_id
WHERE  
gt.diagnosed_gdm = 1
AND ac.is_anemic = 'Yes'
;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 21. Create a function that converts HBA1c levels from IFCC units to DCCT units,
evaluates the HBA1c level during the first visit, and notifies whether it requires attention. */
---------------------------------------------------------------------------------------------------------------------------------
-- https://www.ncbi.nlm.nih.gov/books/NBK348987/#:~:text=IFCC%2C%20International%20Federation%20of%20Clinical,)%20%E2%80%93%2023.5%20mmol/mol.
-- IFCC, International Federation of Clinical Chemistry.
-- https://pubmed.ncbi.nlm.nih.gov/25190675/

-- Definitions: ‘old’ unit = DCCT unit (%); ‘new’ unit = IFCC unit (mmol/mol).
-- Conversion formulas: ‘old’ = (0.0915 × ‘new’) + 2.15%;
-- Assumption: (based on research)
-- As per studies, HbA1c ≥5.9% (≥41 mmol/mol) identified all women with diabetes 
-- and a group at significantly increased risk of adverse pregnancy outcomes.
---------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION evaluate_HBA1c_level_during_first_visit_all()
RETURNS TABLE (
  participant_id INT,
  hba1c_v1_ifcc_value INT,
  hba1c_v1_dcct_value NUMERIC,
  hba1c_v1_status TEXT
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
  	gt.participant_id,
	gt.hba1c_v1,
    (gt.hba1c_v1 * 0.0915) + 2.15 AS hba1c_dcct_value,
    CASE 
      WHEN (gt.hba1c_v1 * 0.0915) + 2.15 >= 5.9 THEN 'Requires Attention'
	  WHEN gt.hba1c_v1 IS NULL THEN 'No data'
      ELSE 'Normal'
    END AS hba1c_status
	FROM glucose_tests gt
	;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM evaluate_HBA1c_level_during_first_visit_all();
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 22.	What is the most common type of anesthesia used (epidural or spinal)?*/
---------------------------------------------------------------------------------------------------------------------------------
SELECT 
epidural_spinal AS common_type_of_anesthesia
FROM maternal_health_info
WHERE epidural_spinal IS NOT NULL AND epidural_spinal <> 'No'
GROUP BY epidural_spinal
ORDER BY COUNT(participant_id) DESC
LIMIT 1
;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 23. Compare the average change in weight for participants 
who received nutritional counseling compared to those who did not. */
--------------------------------------------------------------------------------------------------
-- Assumption: This query needs to be executed after the cleanup of body_compositions table.
-- (which has duplicate records for each patient and was cleaned up as part of question 1)
--------------------------------------------------------------------------------------------------
SELECT 
d.nutritional_counselling,
COUNT(*) AS participant_count,
ROUND(AVG(bc.weight_change_percent)::NUMERIC, 2) AS avg_weight_change_percent
FROM demographics d
INNER JOIN body_compositions bc 
ON d.participant_id = bc.participant_id
WHERE 
bc.weight_change_percent IS NOT NULL
AND d.nutritional_counselling IS NOT NULL
GROUP BY d.nutritional_counselling
ORDER BY d.nutritional_counselling
;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 24. Identify factors most strongly correlated with GDM diagnosis */
-----------------------------------------------------------------------------------------------------------
-- Based on clinical research, factors such as age, BMI, family history of diabetes, 
-- and being categorized as high risk (probably due to pre-existing conditions like PCOS) are known to be 
-- associated with gestational diabetes (GDM). 
-- These variables are available in our dataset under the demographics table.
-- To investigate this, we first extract the values of these factors for participants 
-- diagnosed with GDM. 
-- Then amongst these factors trying to determine the stronger ones.

-- Reference links:
-- https://www.mayoclinic.org/diseases-conditions/gestational-diabetes/symptoms-causes/syc-20355339
-- https://pmc.ncbi.nlm.nih.gov/articles/PMC8128547/
------------------------------------------------------------------------------------------------------------
SELECT  
ROUND(100.0 * COUNT(*) FILTER (
      WHERE d.age_above_30 = 1 AND gt.diagnosed_gdm = 1
      ) / COUNT(*), 2) AS Percentage_Of_age_above_30Patients_With_GDM,  
ROUND(100.0 * COUNT(*) FILTER (
      WHERE d.bmi_kgm2_v1 >= 25 AND gt.diagnosed_gdm = 1
      ) / COUNT(*), 2) AS Percentage_Of_HighBMI_Patients_With_GDM,
ROUND(100.0 * COUNT(*) FILTER (
      WHERE d.family_history = 1 AND gt.diagnosed_gdm = 1
      ) / COUNT(*), 2) AS Percentage_Of_family_history_Patients_With_GDM,
ROUND(100.0 * COUNT(*) FILTER (
      WHERE d.highrisk = 1 AND gt.diagnosed_gdm = 1
      ) / COUNT(*), 2) AS Percentage_Of_highrisk_Patients_With_GDM

FROM demographics d
INNER JOIN glucose_tests gt
ON d.participant_id = gt.participant_id
WHERE gt.diagnosed_gdm = 1;

----------------------------------------------------------------------------------------------
-- Observation/Insight: In the above result, we can see that the percentage of patients with
-- age above 30 is highest followed by high BMI (BMI >= 25) patients,
-- then patients with family history and lastly, highrisk patients
-- Based on this, we can conclude that "Age above 30" is the most strongly correlated factor 
-- for GDM diagnosis at 71.62% , followed by high BMI levels at 66.22%.
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 25. Create a User 'GDM_Read'.GDM_Read has to access only First 100 participants and 
their details without seeing their Race. (Hint : No table creation) */
---------------------------------------------------------------------------------------------------------------------------------
-- Create the user 'GDM_Read'
CREATE USER GDM_Read WITH PASSWORD 'my_password';

-- Creating a view on demographics table excluding the ethnicity column 
-- and getting the first 100 participants using the window function ROW_NUMBER

CREATE VIEW vw_gdm_read_participants 
AS
SELECT participant_id, age_above_30, height_m, bmi_kgm2_v1, smoking, alcohol_intake,
family_history, highrisk, medications, nutritional_counselling -- not including race(ethnicity)
FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY participant_id) AS row_num
  FROM demographics
) AS ranked_participants
WHERE row_num <= 100;


-- Grant access ONLY to the view
GRANT SELECT ON vw_gdm_read_participants TO GDM_Read;

-- Set the role to the new user- GDM_Read to test the access privileges
SET ROLE  GDM_Read;

SELECT * FROM vw_gdm_read_participants;

-- Check permission for demographics table for this new user- GDM_Read
SELECT * FROM demographics;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 26. Compare % of Cesarean in GDM and Non GDM patients */
---------------------------------------------------------------------------------------------------------------------------------
SELECT  
ROUND(100.0 * COUNT(*) FILTER (
      WHERE m.caesarean = 1 AND gt.diagnosed_gdm = 1
      ) / COUNT(*), 2) AS Percentage_Of_CesareanDelivery_Patients_With_GDM,
	  
ROUND(100.0 * COUNT(*) FILTER (
      WHERE m.caesarean = 1 AND (gt.diagnosed_gdm = 0 OR gt.diagnosed_gdm IS NULL)
      ) / COUNT(*), 2) AS Percentage_Of_CesareanDelivery_Patients_Without_GDM	
FROM maternal_health_info m
INNER JOIN  glucose_tests gt 
ON m.participant_id = gt.participant_id;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 27. List the pair of  participants whose  estimated delivery dates are exactly consecutive dates. */
---------------------------------------------------------------------------------------------------------------------------------
SELECT 
p1.participant_id AS participant_1,
p1.edd_v1 AS estimated_delivery_date_1,
p2.participant_id AS participant_2,
p2.edd_v1 AS estimated_delivery_date_2
FROM 
pregnancy_info p1
INNER JOIN pregnancy_info p2
ON p1.edd_v1 = p2.edd_v1 + INTERVAL '1 day'
ORDER BY 
p1.participant_id
;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 28. Identify the Participant count by ethnicity and age as a combination using CTE. */
---------------------------------------------------------------------------------------------------------------------------------
WITH ethnicity_age_criteria AS (
SELECT
participant_id,
ethnicity,
CASE 
      WHEN age_above_30 = 1 THEN 'Above 30'
      ELSE 'Below 30'
END AS age_criteria
FROM demographics
)
SELECT 
ethnicity,
age_criteria,
COUNT(*) AS participant_count
FROM 
ethnicity_age_criteria
GROUP BY 
  ethnicity, age_criteria
ORDER BY 
  ethnicity, age_criteria;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 29.	List the tables where column participant_id  is present. 
(Display column position number with respective table also).  */
---------------------------------------------------------------------------------------------------------------------------------
SELECT 
TABLE_NAME,
ordinal_position AS participant_id_column_position
FROM 
information_schema.columns
WHERE COLUMN_NAME = 'participant_id'
ORDER BY 
TABLE_NAME;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 30. Create A trigger to raise a notice and prevent deletion of a record from view. */
---------------------------------------------------------------------------------------------------------------------------------
-- Create a view for demographics table with participant_id and ethnicity
CREATE VIEW vw_participant_ethnicity
AS
SELECT participant_id, ethnicity
FROM demographics;

-- Create trigger function to raise a notice
CREATE OR REPLACE FUNCTION prevent_view_record_deletion()
RETURNS trigger 
AS $$
BEGIN
  RAISE NOTICE 'Deletion from view vw_participant_ethnicity is not allowed.';
  RETURN NULL; 
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER trg_prevent_view_record_delete
INSTEAD OF DELETE ON vw_participant_ethnicity
FOR EACH ROW
EXECUTE FUNCTION prevent_view_record_deletion();

-- Try to delete record of participant 387

DELETE FROM vw_participant_ethnicity 
WHERE participant_id = 387;

SELECT * FROM vw_participant_ethnicity 
WHERE participant_id = 387;
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 31. Identify infants whose condition improved from critically low to normal.
Use window functions to determine the order based on severity to extent of recovery. */
-----------------------------------------------------------------------------------------
-- Assumptions: Considering the apgar_1_min <= 3 as ‘Critical’ 
-- and apgar_3_min >= 7 as Normal, to find whether the condition improved or not.
-- https://medlineplus.gov/ency/article/003402.htm
-- https://www.healthline.com/health/apgar-score
-----------------------------------------------------------------------------------------
WITH categorize_apgar_scores AS (
SELECT 
participant_id,
apgar_1_min,
apgar_3_min,
CASE 
      WHEN apgar_1_min <= 3 THEN 'Critical'
      WHEN apgar_1_min BETWEEN 4 AND 6 THEN 'Low'
      ELSE 'Normal'
END AS apgar_1_condition,
CASE 
      WHEN apgar_3_min <= 3 THEN 'Critical'
      WHEN apgar_3_min BETWEEN 4 AND 6 THEN 'Low'
      ELSE 'Normal'
END AS apgar_3_condition
FROM 
infant_outcomes  
)
SELECT 
*,
ROW_NUMBER() OVER (ORDER BY apgar_1_min ASC, apgar_3_min DESC) AS recovery_order
FROM 
categorize_apgar_scores
WHERE 
apgar_1_min <= 3      
AND apgar_3_min >= 7; 
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 32. List the Patients whose newborn may need immediate medical attention at  birth time */
------------------------------------------------------------------------------------------------------
-- Assumptions: Considering the apgar_1_min and apgar_min_3 values < 7 and low birth weight (< 2.5) 
-- as the cohort in need of immediate medical attention
-- https://medlineplus.gov/ency/article/003402.htm
-- https://www.healthline.com/health/apgar-score
------------------------------------------------------------------------------------------------------
SELECT 
participant_id,
apgar_1_min,
apgar_3_min,
birth_weight
FROM 
infant_outcomes  
WHERE 
(apgar_1_min IS NOT NULL AND apgar_1_min < 7)
OR (apgar_3_min IS NOT NULL AND apgar_3_min < 7)
OR (birth_weight IS NOT NULL AND birth_weight < 2.5);
---------------------------------------------------------------------------------------------------------------------------------
/* Qn. 33. Calculate the average gestational age at delivery for GDM vs non-GDM pregnancies*/
---------------------------------------------------------------------------------------------------------------------------------
SELECT 
  CASE 
    WHEN gt.diagnosed_gdm = 0 THEN 'Non-GDM'
    WHEN gt.diagnosed_gdm = 1 THEN 'GDM'
  END AS gdm_status,
  ROUND( AVG(
      CASE
        WHEN p.ga_delivery IS NULL THEN NULL
        ELSE p.ga_delivery::double precision
      END)::numeric, 2) AS avg_gestational_age_weeks
FROM pregnancy_info p
JOIN glucose_tests gt ON p.participant_id = gt.participant_id
WHERE gt.diagnosed_gdm IN (0, 1)
GROUP BY gt.diagnosed_gdm;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 34. Calculate the Participants Mean arterial Pressure (MAP) for both Visit 1 and Visit 3.*/
---------------------------------------------------------------------------------------------------------------------------------
--Referred Links:
--https://www.ncbi.nlm.nih.gov/books/NBK538226/#:~:text=The%20definition%20of%20mean%20arterial,is%20influenced%20by%20several%20variables.
-- Conversion Formula: MAP = DBP + ⅓ × (SBP − DBP)
---------------------------------------------------------------------------------------------------------------------------------
SELECT 
    participant_id,
    (diastolic_bp_v1 + (systolic_bp_v1 - diastolic_bp_v1) / 3) AS MAPv1, -- Calculating MAP for Visit 1
    -- Calculating MAP for Visit 3
   CASE
        WHEN diastolic_bp_v3 IS NOT NULL AND systolic_bp_v3 IS NOT NULL THEN
            (diastolic_bp_v3 + (systolic_bp_v3 - diastolic_bp_v3) / 3)
        ELSE
            NULL  -- Return NULL if either diastolic_bp_v3 or systolic_bp_v3 is NULL
    END AS MAPv3
FROM 
    vital_signs;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 35. List pregnancies that exceeded the standard 40 weeks full term and calculate the number of days delayed.*/
---------------------------------------------------------------------------------------------------------------------------------
SELECT 
    participant_id,
    ga_delivery,
    ROUND(((ga_delivery - 40) * 7)) AS days_delayed
FROM 
    pregnancy_info
WHERE 
    ga_delivery > 40;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 36. "Apply lookahead concept to transform medication data and generate new column in the glucose_tests table."*/
--------------------------------------------------------------------------------------------------------------------------------
SELECT*From glucose_tests 
--Generating a new column.
ALTER TABLE glucose_tests
ADD COLUMN therapy_prediction TEXT;
--Update the column with lookahead logic
UPDATE glucose_tests
SET therapy_prediction = CASE
    WHEN glucose_lowering_therapy = 1 OR TRIM(LOWER(insulin_metformnin)) IN ('insulin', 'metformin','metformininsulin')
        THEN 'Already on therapy'
    WHEN glucose_lowering_therapy = 0 AND (
        COALESCE(hba1c_v2, 0) > COALESCE(hba1c_v1, 0)
        OR COALESCE(hba1c_v3, 0) > COALESCE(hba1c_v2, 0)
    )
        THEN 'Need therapy'
    ELSE 'Stable/unknown'
END;
--To view the output
SELECT participant_id, glucose_lowering_therapy, insulin_metformnin,
       hba1c_v1, hba1c_v2, hba1c_v3, therapy_prediction
FROM glucose_tests;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 37. Find the correlation between Vitamin D  levels and GDM diagnosis*/
--------------------------------------------------------------------------------------------------------------------------------
SELECT
  ROUND(CORR(bm.diagnosed_with_vitd_deficiency,
             gt.diagnosed_gdm)::numeric, 5) AS "Correlation"
FROM
  biomarkers bm
JOIN
  glucose_tests gt ON bm.participant_id = gt.participant_id;
--The correlation between Vitamin D  levels and GDM diagnosis is negative (-0.12069) so we can say it's weak. 
--Vitamin-D deficiency and GDM diagnosis hardly vary together.
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 38. Calculate the Cumulative percentage of Insulin medication consumption  for gestational diabetic patients*/
--------------------------------------------------------------------------------------------------------------------------------
SELECT 
    ROUND(
        (COUNT(*) FILTER (WHERE LOWER(insulin_metformnin) LIKE '%insulin%')::DECIMAL
		/ COUNT(*)) * 100, 2) AS cumulative_percentage_insulin
FROM glucose_tests
WHERE diagnosed_gdm = 1
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 39. Count the Patient based on "BMI" category using "Width_bucket" function*/
--------------------------------------------------------------------------------------------------------------------------------
--Referred Links:
--https://neon.tech/postgresql/postgresql-math-functions/postgresql-width_bucket
--https://ellisvalentiner.com/post/discretizing-data-in-postgres-with-width-bucket/ 
-- To keep rows in logical BMI order(clinical categories exactly), using a width-bucket with an array of thresholds. 
--------------------------------------------------------------------------------------------------------------------------------
SELECT
  CASE width_bucket(bmi_kgm2_v1, ARRAY[18.5, 25, 30])
       WHEN 0 THEN 'Underweight'      -- < 18.5
       WHEN 1 THEN 'Normal weight'    -- 18.5 – 24.9
       WHEN 2 THEN 'Overweight'       -- 25 – 29.9
       WHEN 3 THEN 'Obese'            -- ≥ 30
  END            AS "BMI category",
  COUNT(*)       AS "Patient Count"
FROM demographics
WHERE bmi_kgm2_v1 IS NOT NULL
GROUP BY "BMI category"
ORDER BY MIN(bmi_kgm2_v1); 
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 40. Transform the values of edd_estimation_method to replace the abbreviations and handle nulls.*/
--------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW pregnancy_info_transformed AS
SELECT
    participant_id,
    gestational_age_v1,
    ga_delivery,
    edd_v1,
    edd_consistent_with_lmp,
    CASE
        WHEN edd_estimation_method = 'CRL' THEN 'Crown-Rump Length'
        WHEN edd_estimation_method = 'BPD' THEN 'Biparietal Diameter'
        WHEN edd_estimation_method IS NULL THEN 'Unknown'
        ELSE edd_estimation_method
    END AS edd_estimation_method,
    twins,
    delivered_before_36_weeks,
    "Still-birth",
    "Miscarried 10",
    miscarriage_after_28_weeks,
    miscarriage_before_28_weeks
FROM pregnancy_info;
--To check the view
SELECT * FROM pregnancy_info_transformed;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 41. Analyze the impact of GDM on infant outcomes using a composite score*/
--------------------------------------------------------------------------------------------------------------------------------
--Referred Links:
-- https://my.clevelandclinic.org/health/diagnostics/23094-apgar-score
-- Analysis: Infants born to mothers with GDM have a lower average composite score (0.014) compared to 
--infants born to Non-GDM mothers (0.058), indicating fewer complications on average in the GDM group.
/* A possible reason is that effective glucose management in diagnosed-GDM pregnancies reduced complications, 
leading to the lower composite score.*/
--------------------------------------------------------------------------------------------------------------------------------
SELECT 
   CASE WHEN gt.diagnosed_gdm = 1 THEN 'GDM' ELSE 'Non-GDM' END  AS gdm_status,
    COUNT(*) AS total_infants,
      ROUND( AVG(
            CASE WHEN i.apgar_1_min < 7 THEN 1 ELSE 0 END +
            CASE WHEN i.apgar_3_min < 7 THEN 1 ELSE 0 END +
            COALESCE(i.birth_injury_fracture, 0) +
            CASE WHEN i.birth_weight < 2.5 THEN 1 ELSE 0 END +
            COALESCE(i."Fetal hypoglycaemia 10", 0) +
            COALESCE(i."Fetal jaundice 10", 0)
        ), 3
    ) AS avg_composite_score
FROM infant_outcomes i
JOIN glucose_tests gt ON i.participant_id = gt.participant_id
WHERE gt.diagnosed_gdm IS NOT NULL
GROUP BY gdm_status
ORDER BY gdm_status;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 42. Retrieve a list of participants who share the same estimated delivery due date with at least one other participant*/
--------------------------------------------------------------------------------------------------------------------------------
SELECT p.participant_id, p.edd_v1
FROM pregnancy_info p
WHERE p.edd_v1 IN (
    SELECT edd_v1
    FROM pregnancy_info
    GROUP BY edd_v1
    HAVING COUNT(*) > 1)
ORDER BY p.edd_v1, p.participant_id;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 43. Of all miscarriages records, what percentage were currently using tobacco or drinking and what % were not?*/
--------------------------------------------------------------------------------------------------------------------------------
WITH miscarriage_participants AS (
    SELECT DISTINCT participant_id
    FROM pregnancy_info
    WHERE "Miscarried 10" = 1
       OR miscarriage_after_28_weeks = 1
       OR miscarriage_before_28_weeks = 1),
miscarriage AS (
    SELECT smoking, alcohol_intake
    FROM demographics
    WHERE participant_id IN (SELECT participant_id FROM miscarriage_participants))
SELECT
    COUNT(*) AS "Total miscarriages",
    ROUND(100.0 * COUNT(*) FILTER (WHERE smoking = 'Current' OR alcohol_intake = 1) / COUNT(*), 2) AS "Percentage of using tobacco or drinking",
    ROUND(100.0 * COUNT(*) FILTER (WHERE NOT (smoking = 'Current' OR alcohol_intake = 1)) / COUNT(*), 2) AS "Percentage of not using"
FROM miscarriage;
/*
SELECT*from pregnancy_info
where "Miscarried 10" = 1
select * from demographics
where alcohol_intake =0*/
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 44. Using window functions, identify all participants whose pulsation significantly considered an outlier
Hint: Threshold greater than 20 bpm "*/
--------------------------------------------------------------------------------------------------------------------------------
SELECT
    participant_id,
    pulse_v1,
    pulse_v3,
    pulse_diff,
    'Outlier' AS outlier_status
FROM (
    SELECT
        participant_id,
        pulse_v1,
        pulse_v3,
        ABS(pulse_v3 - pulse_v1) AS pulse_diff,
        MAX(CASE WHEN ABS(pulse_v3 - pulse_v1) > 20 THEN 1 ELSE 0 END)
          OVER (PARTITION BY participant_id) AS is_outlier
    FROM vital_signs
) AS derived
WHERE is_outlier = 1
ORDER BY participant_id;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 45. Display the participant_id from 100 to 200 without using where condition*/
--------------------------------------------------------------------------------------------------------------------------------
--Assumptions: Partipant_id is available in all tables I'm taking demographics table.
SELECT 
  participant_id
FROM 
  demographics
GROUP BY 
  participant_id
HAVING 
  participant_id BETWEEN 100 AND 200
ORDER BY 
  participant_id ASC;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 46. Create a Backup table  by using existing demographics table.List the differences 
observed between backup table and Base table*/
--------------------------------------------------------------------------------------------------------------------------------
-- Creating a backup table demographics_backup with the same data as demographics
CREATE TABLE demographics_backup AS
SELECT * FROM demographics;

-- Finding any participant_id in demographics that is not in demographics_backup 
SELECT * FROM demographics
WHERE participant_id NOT IN (SELECT participant_id FROM demographics_backup);

-- Listing columns of both tables to compare their structure
SELECT table_name, column_name
FROM information_schema.columns 
WHERE table_name IN ('demographics', 'demographics_backup')
ORDER BY column_name, table_name;

-- Comparing column names and data types between demographics and demographics_backup for differences
SELECT 
    d.column_name AS demographics_columns,
    d.data_type AS demographics_data_type,
    db.data_type AS backuptable_data_type
FROM 
    information_schema.columns d
LEFT JOIN 
    information_schema.columns db 
    ON d.column_name = db.column_name
   AND db.table_name = 'demographics_backup'
WHERE 
    d.table_name = 'demographics';
	
-- Listing constraints (primary keys, foreign keys) on demographics
SELECT 
    conname AS constraint_name,
    contype AS constraint_type,
    conrelid::regclass AS table_name
FROM pg_constraint
WHERE conrelid = 'demographics'::regclass;

-- List constraints on demographics_backup
SELECT 
    conname AS constraint_name,
    contype AS constraint_type,
    conrelid::regclass AS table_name
FROM pg_constraint
WHERE conrelid = 'demographics_backup'::regclass;

-- List indexes on demographics
SELECT 
    indexname, 
    indexdef
FROM 
    pg_indexes
WHERE 
    tablename = 'demographics';

-- List indexes on demographics_backup
SELECT 
    indexname, 
    indexdef
FROM 
    pg_indexes
WHERE 
    tablename = 'demographics_backup';

/* Differences observed between two tables are: 
1. There is no constraints in demographics_backup table. 
2. There is no index in demogaphics_backup table.
Only copies the data and column definitions copied from the base(demographics) table.*/
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 47. Create function and input the participant id, generate a 16-digit code with characters or digits until it reaches a total length of 16.
Also, display the number of characters added during this process.*/
--------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION generate_participant_code (participant_id INTEGER)
RETURNS TABLE (
    generated_code   TEXT,      
    characters_added INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    v_generated_code   TEXT    := participant_id::TEXT;   
    v_Chars_added  INTEGER := 0;
    target INTEGER := 16;
    possible_digits TEXT := '0123456789';
    i INTEGER;
	v_participant_count INTEGER;
BEGIN
	SELECT COUNT(*) INTO v_participant_count FROM demographics d;
    RAISE NOTICE 'Total participants in demographics: %', v_participant_count;
	
    /* Accept IDs 1-600 only */
    IF participant_id BETWEEN 1 AND 600 THEN
        WHILE LENGTH( v_generated_code) < target LOOP
            i      := FLOOR(random() * LENGTH(possible_digits) + 1);
            v_generated_code :=  v_generated_code || substr(possible_digits, i, 1);
            v_Chars_added := v_Chars_added + 1;
        END LOOP;

        RAISE NOTICE 'Generated Code: %',  v_generated_code;
        RAISE NOTICE 'Number of characters added: %', v_Chars_added;

        generated_code   :=  v_generated_code;
        characters_added := v_Chars_added;
        RETURN NEXT;
    ELSE
        RAISE NOTICE 'Participant ID % is not Found.', participant_id;
        generated_code   := NULL;
        characters_added := 0;
        RETURN NEXT;
    END IF;
END;
$$;

/*DROP FUNCTION generate_participant_code(INT)*/

SELECT * FROM generate_participant_code( -1)
SELECT * FROM generate_participant_code(0)
SELECT * FROM generate_participant_code(123)
SELECT * FROM generate_participant_code(60578)
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 48. Display the last inserted row in the demographics table without using limit.*/
--------------------------------------------------------------------------------------------------------------------------------
--Assumptions: 
--participant_id is auto-increment column
----------------------------------------------
SELECT * FROM demographics
ORDER BY participant_id DESC
FETCH FIRST ROW ONLY;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 49. Count of patients by first letter of insulin_metformnin column.Replace blank values to Unknown.*/
--------------------------------------------------------------------------------------------------------------------------------
	SELECT COUNT(Participant_id)
	from glucose_tests	
	where insulin_metformnin ILIKE 'i%';

	--Replace blank values to Unknown.
	UPDATE  glucose_tests	
	SET insulin_metformnin  = 'Unknown'
	WHERE insulin_metformnin IS NULL;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 50. "Create a Index on Ethnicity column. Check whether index is used in below Query:
		select ethnicity,count(participant_id) 
	 	from public.demographics
	 	group by ethnicity.
	Make sure Above Query to use the index" */
--------------------------------------------------------------------------------------------------------------------------------	
--Create a Index on Ethnicity column
	CREATE INDEX idx_demographics
	ON demographics(ethnicity);
--Check whether index is used in below Query
	EXPLAIN ANALYZE SELECT  ethnicity,COUNT(participant_id) 
	FROM public.demographics
	GROUP BY  ethnicity; -- This is simple Sequential scan
--Make sure Above Query to use the index
	EXPLAIN ANALYZE SELECT ethnicity,COUNT(participant_id) 
	FROM public.demographics
	WHERE ethnicity = 'Asian'
	GROUP BY  ethnicity; -- This is how index works
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 51. Calculate the conception date or Last menstrual for all participants. Generate new attribute */
--------------------------------------------------------------------------------------------------------------------------------
	SELECT participant_id, "US EDD",
	("US EDD" - INTERVAL '266 days'):: DATE AS conception_date --conception date
	FROM documentation_track
	ORDER BY participant_id;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 52. Display different set of 10 patients (every time) who were diagnosed with gestational diabetes 
	from their demographic details.*/
--------------------------------------------------------------------------------------------------------------------------------
	SELECT participant_id,ethnicity,age_above_30 as age, highrisk 
	FROM demographics
	WHERE highrisk = 1
	ORDER BY RANDOM()
	LIMIT 10;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 53. Display list of patients with abnormal Alt_change % and diagnosed with vitamin D deficiency.*/
--------------------------------------------------------------------------------------------------------------------------------
	SELECT  participant_id ,alt_change_percent,vitd_supplements 
	FROM biomarkers 
	WHERE (alt_change_percent  < 5.9  or  alt_change_percent > 97.1) -- BETWEEN 2 AND 33
	AND vitd_supplements =1 ;
	-- https://www.yorkhospitals.nhs.uk/seecmsfile
	-- /?id=6821#:~:text=The%20following%20trimester%2Dspecific%20reference,
	-- biochemical%20profile%20of%20uncomplicated%20pregnancy
	-- calculated based on the non-pregnant upper limit (34 U/L = 100%).	
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 54. What is the distribution of participants by ethnicity and their GDM  status (either 'gdm' or 'non-gdm') 
	in the database? */
--------------------------------------------------------------------------------------------------------------------------------
	SELECT 
	d.ethnicity, 
	CASE 
		WHEN gt.diagnosed_gdm = 1 THEN 'GDM'
		WHEN gt.diagnosed_gdm = 0 THEN 'Non-GDM'
	END AS diagnosed_gdm_status,
	       COUNT (gt.participant_id) AS total_participant 
	FROM glucose_tests gt
	JOIN demographics d ON d.participant_id = gt.participant_id
	WHERE gt.diagnosed_gdm IS NOT NULL
	GROUP BY d.ethnicity, gt.diagnosed_gdm;
	-- ORDER BY gt.diagnosed_gdm;
----------------------------------------------------------------------------------------------------------------------------------
/* Qn. 55. Display all the details of 2nd tallest participant details using windows function */
----------------------------------------------------------------------------------------------------------------------------------
	WITH cte AS(
				SELECT participant_id, height_m,
			           DENSE_RANK() OVER (ORDER BY height_m DESC) AS height_rank
		        FROM demographics  
	)
	SELECT	participant_id,height_m,height_rank
	FROM cte
	WHERE height_rank=2; --2nd tallest participant details
-----------------------------------------------------------------------------------------------------------------------------------
/* Qn. 56. Create a trigger that raises a notice when trying to insert a duplicate participant_id into the demographics table.
	Provide a screenshot of the test result.*/
------------------------------------------------------------------------------------------------------------------------------------
	CREATE OR REPLACE FUNCTION insert_participant_id_before_insert()
	RETURNS TRIGGER 
	LANGUAGE PLPGSQL
	AS
	$$
		BEGIN
			IF NEW.participant_id = OLD.participant_id 
			THEN
-- Triggers a notice for duplicate participant_id in the demographics table.
			RAISE NOTICE 'Duplicate participant_id already exist %',participant_id;
			END IF;
		RETURN NEW;
		END;
	$$;
--Create a trigger 
	CREATE TRIGGER before_insert
	BEFORE INSERT
	ON demographics 
	FOR EACH ROW
	EXECUTE FUNCTION insert_participant_id_before_insert();
-- insert a participant_id
	INSERT INTO demographics(participant_id) VALUES (175); 
------------------------------------------------------------------------------------------------------------------------------------
/* Qn. 57. Compare the number of participants who signed the form on each day of the week 
	and identify the day with the highest number of unique participants.*/
------------------------------------------------------------------------------------------------------------------------------------
	SELECT 
		TO_CHAR(date_form_signed, 'Day') AS day_of_the_week,
		COUNT (DISTINCT participant_id) AS unique_participants
	FROM documentation_track
	GROUP BY day_of_the_week
	ORDER BY unique_participants DESC
	LIMIT 1;
------------------------------------------------------------------------------------------------------------------------------------	
/* Qn. 58. What is the standard deviation of 'U creatinine_V1'? Display the result in two decimal places.*/
------------------------------------------------------------------------------------------------------------------------------------
	SELECT ROUND(STDDEV("U creatinine_V1")::numeric,2) 
	AS STDDEV_U_creatinine_V1
	FROM kidney_function;
------------------------------------------------------------------------------------------------------------------------------------	
/* Qn. 59. Create a Range Partition and show us how the partition is used in a Query.*/
------------------------------------------------------------------------------------------------------------------------------------
-- Create table partition_birth_weigth by taking fields from exsting infant_outcomes
	Create table partition_birth_weigth 
	( participant_id int,
	  birth_weight  double precision,
	PRIMARY KEY(participant_id, birth_weight)) 
	PARTITION BY RANGE (birth_weight);
	
-- Create 1st partition on the partition_birth_weigth table 
	CREATE TABLE extremely_low_bw PARTITION OF partition_birth_weigth 
	    FOR VALUES FROM (0.0) TO (1.0);
		
-- Create 2nd partition on the partition_birth_weigth table 
	CREATE TABLE very_low_bw PARTITION OF partition_birth_weigth 
	    FOR VALUES FROM (1.0) To (1.5);
		
-- Create 3rd partition on the partition_birth_weigth table 
	CREATE TABLE low_bw PARTITION OF partition_birth_weigth 
	    FOR VALUES FROM (1.5) To (2.5);
		
-- Create 4th partition on the partition_birth_weigth table 
	CREATE TABLE normal_bw PARTITION OF partition_birth_weigth 
	FOR VALUES FROM (2.5) To (4.0);
	
-- Create 5th partition on the partition_birth_weigth table 
	CREATE TABLE high_bw PARTITION OF partition_birth_weigth 
	FOR VALUES FROM (4.0) To (6.0)  ;
	
--insert data from infant_outcomes into partition_birth_weigth 
	INSERT INTO partition_birth_weigth (participant_id, birth_weight)
	SELECT participant_id, birth_weight 
	FROM infant_outcomes 
	WHERE birth_weight  is not null;
	
-- Retrieve  birth_weight for extremely_low_bw  for January
--These queries will only access the appropriate partitions, resulting in improved query performance.
	SELECT * FROM  partition_birth_weigth  WHERE birth_weight >= 0.0 AND birth_weight  < 1.0;
------------------------------------------------------------------------------------------------------------------------------------	
/* Qn. 60. Calculate the BMI for Visit 3 and Display the Highest BMI  and their participant details.*/
------------------------------------------------------------------------------------------------------------------------------------	
	DELETE FROM body_compositions
    WHERE ctid NOT IN (
	SELECT MAX(ctid)
	FROM body_compositions
	GROUP BY  participant_id, weight_v1, weight_v3, weight_change_percent, abdominal_circumference_v3 );
	
	SELECT d.participant_id,d.height_m,b.weight_v3,
		  ROUND(b.weight_v3/(d.height_m*d.height_m)) AS BMI_v3
	FROM demographics d
	JOIN body_compositions b ON d.participant_id = b.participant_id
	WHERE weight_v3 IS NOT NULL
	ORDER BY BMI_v3 DESC
	LIMIT 1;
------------------------------------------------------------------------------------------------------------------------------------	
/* Qn. 61. How do we gather statistics of table and check when it was done before.*/
------------------------------------------------------------------------------------------------------------------------------------	
	SELECT schemaname, relname AS table_name,last_analyze,last_autoanalyze
	FROM pg_stat_all_tables
 	WHERE relname IN ('demographics','kidney_function','pregnancy_info','biomarkers');
------------------------------------------------------------------------------------------------------------------------------------	
/* Qn. 62. Create a stored procedure that calculates the average OGTT value and compares it against a specified glucose threshold.
	If the average exceeds the threshold, classify the participant as "Gestational diabetes is suspected"*/
------------------------------------------------------------------------------------------------------------------------------------	
--Create a stored procedure that calculates the average OGTT value 
	CREATE OR REPLACE PROCEDURE gd_status(threshold Double Precision )
	LANGUAGE plpgsql
	AS
	$$
	DECLARE              -- Declare a cursor for the select statement
		gd_cursor CURSOR FOR
		SELECT  participant_id, 
				CASE 
	   				 WHEN num_nonnulls("0H_OGTT_Value", "1H_OGTT_Value", "2H_OGTT_Value") = 0 THEN NULL
					 WHEN 
						ROUND (
							(
										COALESCE ("0H_OGTT_Value", 0) + 
										COALESCE ("1H_OGTT_Value", 0) + 
										COALESCE("2H_OGTT_Value", 0)
							 ) ::NUMERIC  -- and compares it against a specified glucose threshold.
						/ num_nonnulls("0H_OGTT_Value", "1H_OGTT_Value", "2H_OGTT_Value")::NUMERIC ,2)> threshold
					THEN 'suspected' 				
					ELSE 'not suspected' 
					END as diagnosed_by_gestational_diabetes
		FROM glucose_tests
		WHERE "0H_OGTT_Value" IS NOT NULL 
		and  "1H_OGTT_Value" IS NOT NULL 
		and "2H_OGTT_Value" IS NOT NULL 
		order by diagnosed_by_gestational_diabetes DESC;
						
		gd_record RECORD;   -- record variable to hold fetched row
		                                       --row_variable table_name%ROWTYPE;
		is_records boolean:= false;     -- flag to check if any records are found
		   
	BEGIN     
	  OPEN gd_cursor; -- Open the cursor
	  RAISE NOTICE '	participant_id		Gestational diabetes status '; 
	  LOOP                           -- loop to fetch records from the cursor
	    FETCH gd_cursor INTO gd_record ;
	  	EXIT WHEN NOT FOUND;    -- Exit the loop if no more records are found
		  
	  	is_records:= true;           -- if we found at least one customer
		  
		IF gd_record.diagnosed_by_gestational_diabetes = 'suspected' THEN
	  		RAISE NOTICE  '	% 			suspected' ,gd_record.participant_id;
		ELSE  --gd_record.diagnosed_by_gestational_diabetes = 'not suspected' 
			RAISE NOTICE '	%			not suspected',gd_record.participant_id;
		END IF;
	                                    
	  END LOOP;
	 IF NOT is_records THEN  -- if no participant were found, display a message
	 	RAISE NOTICE 'no participant found : %',gd_record.participant_id; 
	 END IF;
	 CLOSE gd_cursor ;   -- close the cursor
	END
	$$;
	-- Call the procedure
	CALL gd_status(7.78);
-------------------------------------------------------------------------------------------------------------------------------------
/* Qn. 63. Calculate Number of days difference between expected delivery date and ultrasound EDD */
-------------------------------------------------------------------------------------------------------------------------------------	
	SELECT pi.participant_id,pi.edd_v1,dt."US EDD",
		  (pi.edd_v1 - dt."US EDD") AS days_difference
	FROM pregnancy_info pi
	JOIN documentation_track dt
	ON pi.participant_id = dt.participant_id
	ORDER BY pi.participant_id ;
-------------------------------------------------------------------------------------------------------------------------------------
/* Qn. 64. Estimate the follow up visit dates for all participants for each trimester and display them.*/
-------------------------------------------------------------------------------------------------------------------------------------
	SELECT participant_id, edd_v1,
		(edd_v1 - INTERVAL '40 weeks') :: DATE AS Visit_1_date,
		(edd_v1 - INTERVAL '26 weeks') :: DATE AS Visit_2_date,
		(edd_v1 - INTERVAL '12 weeks') :: DATE AS Visit_3_date
	FROM pregnancy_info;
------------------------------------------------------------------------------------------------------------------------------------
/* Qn. 65. Show the position of letter 'n' in the insulin_metformnin column.Replace blank values to Unknown.
	 List only distinct values.Hint:'n' is not case sensitive
------------------------------------------------------------------------------------------------------------------------------------
Assumption:
	1) The requirement states "show the position of letter 'n' in the insulin_metformnin column." Based on this, 
	   I have displayed the position of the first occurrence of the letter 'n' (case-insensitive) in the column.
	2) I replced the blank and null values to the 'Unknown' as per the requirement.
*/
------------------------------------------------------------------------------------------------------------------------------------
SELECT
	insulin_metformnin,POSITION('n' IN LOWER(INSULIN_METFORMNIN)) AS "1st occurance of nth position"
FROM
	(
		SELECT DISTINCT
			CASE
				WHEN INSULIN_METFORMNIN IS NULL
				OR INSULIN_METFORMNIN = '' THEN 'Unknown'
				ELSE INSULIN_METFORMNIN
			END
		FROM
			GLUCOSE_TESTS
	);
-----------------------------------------------------------------------------------------------------------------------------------
/* Qn. 66. Create a function to load data from an existing table into a new table, inserting records in batches of 100.*/
-----------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS NEW_TABLE;

-- recreates the infant_outcome structure, constraints, datatypes into the new_table
CREATE TABLE NEW_TABLE (LIKE INFANT_OUTCOMES INCLUDING ALL);

-- Fucntion that inserts 100 records at a time into new_table from the existing table
CREATE OR REPLACE FUNCTION LOAD_BATCH_DATA (EXISTING_TABLE_NAME VARCHAR) 
RETURNS VOID 
LANGUAGE PLPGSQL 
AS $$
declare
	batch_offsets integer := 0;
	insert_query varchar;
	inserted_row_counts integer;
	total_rows integer := 0;
begin
	loop
		insert_query := format('insert into new_table select * from %I limit 100 offset %s', existing_table_name, batch_offsets);
		execute insert_query;
		
		get diagnostics inserted_row_counts = row_count;

		exit when inserted_row_counts = 0;

		total_rows := total_rows + inserted_row_counts;
		raise notice'Inserted batch size: % (total records so far: %)', inserted_row_counts, total_rows;

		batch_offsets := batch_offsets + 100;
		
	end loop;
end;
$$;

-- Fuction calling
SELECT *
FROM LOAD_BATCH_DATA ('infant_outcomes');

SELECT *
FROM NEW_TABLE;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 67. Compare the average change in hemoglobin levels based on ethnicity using window function */
--------------------------------------------------------------------------------------------------------------------------------
SELECT
    d.participant_id,
    d.ethnicity,
    b.hb_v1,
    b.hb_v3,
    round((b.hb_v3 - b.hb_v1)::integer,2) AS hb_change,
    round(AVG(b.hb_v3 - b.hb_v1) OVER (PARTITION BY d.ethnicity)::integer,2) AS avg_hb_change_per_ethnicity
FROM
    demographics d
JOIN
    biomarkers b ON d.participant_id = b.participant_id
WHERE
    b.hb_v1 IS NOT NULL AND b.hb_v3 IS NOT NULL;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 68. List all the participants whose expected Delivery Date is Weekend. */
--------------------------------------------------------------------------------------------------------------------------------
	-- the day of the week reprsented as number in Postgresql(0 to 6)
	-- 0 represents Sunday and 6 represents Saturday
	-- to_char converts the day number to text
--------------------------------------------------------------------------------------------------------------------------------
SELECT participant_id, 
	   CASE WHEN EXTRACT(dow FROM edd_v1) IN(0,6) THEN to_char(edd_v1, 'day') 
	   ELSE 'weekday' END AS day_name
FROM pregnancy_info
WHERE EXTRACT(dow FROM edd_v1) IN(0,6);

--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 69. Calculate the percentage of GDM patients using only insulin medication */
--------------------------------------------------------------------------------------------------------------------------------
SELECT round(COUNT(participant_id)*100.0/(SELECT COUNT(participant_id) 
										   FROM glucose_tests WHERE diagnosed_gdm = 1),0) 
										   AS "only_insulin_taking_percentage"
FROM glucose_tests 
WHERE insulin_metformnin = 'Insulin';

--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 70. Compare Ultrasound delivery date and edd by Lmp and Graph the Stacked Line chart.*/
--------------------------------------------------------------------------------------------------------------------------------
--Unable to add the ultrasound edd and edd by lmp  in y axis, so calculated the date difference 
--between the Ultrasound delivery date and edd by LMP date, generated the stacked line chart.

--The acceptable days difference between US_EDD and EDD by LMP is 10 days.(7-10)

--But our dataset has 47 patient records, more than the acceptable days.

--Referred Website:
--https://www.acog.org/clinical/clinical-guidance/committee-opinion/articles/2017/05/methods-for-estimating-the-due-date#:~:text=If%20dating%20by%20ultrasonography%20performed,to%20correspond%20with%20the%20ultrasonography
--------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM(SELECT "US EDD",
		edd_v1, 
		d.participant_id,
		"US EDD" - edd_v1 AS date_diff_usedd_lmpedd 
FROM documentation_track d 
JOIN pregnancy_info p ON d.participant_id = p.participant_id) 
WHERE date_diff_usedd_lmpedd > 10 OR date_diff_usedd_lmpedd < -10;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 71. What proportion of participants diagnosed with gestational diabetes mellitus (GDM) have a 
  family or their own previous history of the condition? */
--------------------------------------------------------------------------------------------------------------------------------
SELECT proportion_of_history, 
	   round(proportion_of_history * 100,0) AS proportion_percentage
FROM
	(SELECT 
		round(
			(SELECT COUNT(g.participant_id) 
		   	 FROM glucose_tests g JOIN demographics d ON g.participant_id = d.participant_id
		     WHERE diagnosed_gdm = 1 AND (highrisk = 1 OR family_history =1))*1.0
		   /
		  	(SELECT COUNT(g.participant_id) 
		  	FROM glucose_tests g JOIN demographics d ON g.participant_id = d.participant_id 
		  	WHERE diagnosed_gdm = 1 )
	     ,2) AS proportion_of_history
	)AS sub_query;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 72.  1) Create a backup of the demographic table that is accessible only for the current session..
    		2) In a new session ,display the name of the  schema name and backup table ,created (Attach Both the screen shots)"*/
--------------------------------------------------------------------------------------------------------------------------------
-- Temporary table is a table that exists only during a database session. It is created and used within a single 
 --datatbse session and the temporary table is automatically dropped at the end of the session.

--Referred Website:
--https://neon.tech/postgresql/postgresql-tutorial/postgresql-temporary-table
--------------------------------------------------------------------------------------------------------------------------------
CREATE TEMPORARY TABLE temp_doc_track AS 
SELECT * FROM documentation_track;

SELECT * FROM temp_doc_track;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 73.	What percentage of participants diagnosed with gestational diabetes mellitus (GDM) 
	are using insulin, insulin & metformin and no-medication? */
--------------------------------------------------------------------------------------------------------------------------------
WITH gdm_participant AS 
	(SELECT * 
    FROM glucose_tests 
    WHERE diagnosed_gdm = 1
	),
total_GDM AS
	(SELECT COUNT(participant_id) AS gdm_patient_count 
	FROM glucose_tests WHERE diagnosed_gdm = 1
	)
	SELECT 'Insulin' AS Medication_Type, COUNT(participant_id)*100
	/ (SELECT gdm_patient_count FROM total_gdm) AS Participant_taking_percentage 
	FROM gdm_participant WHERE insulin_metformnin = 'Insulin' 
UNION
	SELECT 'Insulin & Metformin' AS Medication_Type, COUNT(participant_id)*100 
	/ (SELECT gdm_patient_count FROM total_gdm) AS Participant_taking_percentage 
	FROM gdm_participant WHERE insulin_metformnin = 'MetforminInsulin' 
UNION
	SELECT 'No Medication' AS Medication_Type, COUNT(participant_id)*100 
	/ (SELECT gdm_patient_count FROM total_gdm) AS Participant_taking_percentage
	FROM gdm_participant WHERE insulin_metformnin = 'No'
UNION 
	SELECT 'Metformin' AS Medication_Type, COUNT(participant_id)*100 
	/ (SELECT gdm_patient_count FROM total_gdm) AS Participant_taking_percentage 
	FROM gdm_participant WHERE insulin_metformnin = 'Metformnin';
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 74. 	"What are the ways to optimize below Query.
		select * from 
		public.pregnancy_info p, public.demographics d
		where extract (year from edd_v1)='2015'
		and p.participant_id=d.participant_id and d.ethnicity='White'  */
--------------------------------------------------------------------------------------------------------------------------------
		/*1. Optimization 1:
		By default the extract(year from date) returns numeric so changed the year
		from string to number reduce the type casting overhead

		2. Optimization 2:
		Between and is more effective than the extract funcion.
		Also the extract function doesn't use created index.

		3. Tried creating indexes, but the query engine doesn't use index though indices are there,
		instead used sequential scanning. It's because of the size of the dataset. */
--------------------------------------------------------------------------------------------------------------------------------
explain analyse
select * from 
public.pregnancy_info p, public.demographics d
where extract (year from edd_v1)='2015'
and p.participant_id=d.participant_id and d.ethnicity='White';

explain analyse
select * from 
public.pregnancy_info p, public.demographics d
where extract (year from edd_v1) = 2015
and p.participant_id=d.participant_id and d.ethnicity='White';

explain analyse
SELECT * FROM 
pregnancy_info p 
JOIN demographics d ON p.participant_id = d.participant_id
WHERE edd_v1 between '01-01-2015' and '12-31-2015'
AND d.ethnicity='White';

-- creating index
CREATE INDEX index_ethniciy on demographics(ethnicity);

CREATE INDEX index_edd_2015
ON pregnancy_info (participant_id)
WHERE edd_v1 >= '2015-01-01' AND edd_v1 <= '2015-12-31';
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 75. Display preeclampsia occurrence across different gestational hypertension statuses using cross tab.
--------------------------------------------------------------------------------------------------------------------------------
 1) Preeclampsia is a serious medical condition that occur usually after 20th week 
     of pregnancy. So I have  taken the Visit 3 blood pressure values.
 2) It complicates 5% - 8% of all the birth in the USA.
 3) The causes of Preeclampsia is, no one entirely sure. Some researchers beleive 
      it may happen due to problem with blood supply to the placenta.
 4) –Visit 3 Systolic and Diastolic columns, which have 74 Null values. 
Referred websites:
 - hypertension ranges referred from the following website:
https://www.drugs.com/cg/hypertension-during-pregnancy.html

About preeclampsia referred the following website:
https://my.clevelandclinic.org/health/diseases/17952-preeclampsia
*/
--------------------------------------------------------------------------------------------------------------------------------
WITH hypertension_statuses as 
(select participant_id,  
CASE 
    WHEN diastolic_bp_v3 > 120 OR systolic_bp_v3 > 180 THEN '5. Hypertension_crisis'
    WHEN diastolic_bp_v3 >= 90 OR systolic_bp_v3 >= 140 THEN '4. Hypertension_stage2'
    WHEN diastolic_bp_v3 >= 80 OR systolic_bp_v3 >= 130 THEN '3. Hypertension_stage1'
    WHEN systolic_bp_v3 >= 120 THEN '2. Elevated'
    ELSE '1. Normal'
END AS hypertension_status
FROM vital_signs)

SELECT hypertension_status, 
		COUNT(CASE WHEN "Pre-eclampsia" = 1 THEN 1 END) AS Preclampsia_count,
		COUNT(CASE WHEN "Pre-eclampsia" = 0 THEN 1 END) AS Non_preclampsia_count
FROM maternal_health_info m 
JOIN hypertension_statuses h ON m.participant_id = h.participant_id
GROUP BY hypertension_status;

--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 76. Postgres supports extensibility for JSON querying. Prove it.*/
--------------------------------------------------------------------------------------------------------------------------------
-- creating a table with json type column
CREATE TABLE participants(id serial, info json);

-- inserting values 
INSERT INTO participants(info) 
VALUES('{"name": "Chithara", "age": 35, "address":{"city" : "sunnyvale"}}'),
	  ('{"name" : "Isai","age" : 32, "address":{"city": "Bellevue"}}');

-- querying the json type column

SELECT info -> 'name' AS participant_name, 
	   info -> 'address' ->>'city' AS city_name 
FROM participants;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 77.	Display participants whose Vitamin D levels decreased by more than 50% between visit 1 and visit 3.*/
--------------------------------------------------------------------------------------------------------------------------------
	-- The Visit 1 Vitamin D column has 455 NULL values out of 600.
	-- The Visit 3 Vitamin D column has 352 NULL values out of 600.
	-- A total of 476 rows have NULL values in either Visit 1 or Visit 3.
	-- Therefore, only 124 rows have non-NULL values in both visits.
	-- We will use these 124 participants to assess the change in Vitamin D levels
   	-- between  Visit 1 and Visit 3
--------------------------------------------------------------------------------------------------------------------------------
WITH not_null_participants AS
(	SELECT participant_id, "25 OHD_V1", "25 OHD_V3"
	FROM biomarkers 
	WHERE "25 OHD_V3" IS NOT NULL AND "25 OHD_V1" IS NOT NULL
)
SELECT participant_id, "25 OHD_V1", "25 OHD_V3", 
	   ROUND((("25 OHD_V1" - "25 OHD_V3") / "25 OHD_V1")::NUMERIC * 100,2) AS percentage_drop
FROM not_null_participants
WHERE ("25 OHD_V1" - "25 OHD_V3") / "25 OHD_V1" * 100  > 50;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 78.	Among participants with elevated OGTT results, what are the highest, lowest, average HbA1c values at visit 3 ? */
--------------------------------------------------------------------------------------------------------------------------------
SELECT MAX(hba1c_v3) AS max_hba1c, 
	   MIN(hba1c_v3) AS min_hba1c, 
	   round(AVG(hba1c_v3),2) AS avg_hba1c 
FROM glucose_tests 
WHERE ogtt_high_10 = 1;
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 79. Create a stored procedure to fetch past and current GDM status and their birth outcome. Call the procedure recursively. 
	 If the participant GDM is 'Yes'.*/
--------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gdm_status_and_birth_outcomes(pid int)
RETURNS TABLE (
    participant_id INT,
    previous_gdm int,
    diagnosed_gdm int,
    apgar_1_min INT,
    apgar_3_min INT,
    birth_weight DOUBLE PRECISION,
    birth_injury_fracture int,
    fetal_hypoglycaemia int,
    fetal_jaundice int,
    gestational_age text,
    ga_delivery DOUBLE PRECISION,
    delivered_before_36_weeks int,
    still_birth int
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.participant_id,
        s.previous_gdm,
        g.diagnosed_gdm,
        io.apgar_1_min,
        io.apgar_3_min,
        io.birth_weight,
        io.birth_injury_fracture,
        io."Fetal hypoglycaemia 10",
        io."Fetal jaundice 10",
        p.gestational_age_v1,
        p.ga_delivery,
        p.delivered_before_36_weeks,
        p."Still-birth"
    FROM glucose_tests g
    INNER JOIN pregnancy_info p ON g.participant_id = p.participant_id
    INNER JOIN infant_outcomes io ON g.participant_id = io.participant_id
    INNER JOIN screening s ON g.participant_id = s.participant_id
    WHERE g.participant_id = pid;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION call_for_all_gdm()
RETURNS TABLE (
    participant_id INT,
    previous_gdm INT,
    current_gdm INT,
    birth_weight DOUBLE PRECISION,
    apgar1 INT,
    apgar3 INT,
    still_birth INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        res.participant_id,
        res.previous_gdm,
        res.diagnosed_gdm,
        res.birth_weight,
        res.apgar_1_min,
        res.apgar_3_min,
        res.still_birth
    FROM glucose_tests gt
    JOIN LATERAL gdm_status_and_birth_outcomes(gt.participant_id) AS res ON true
    WHERE gt.diagnosed_gdm = 1;
END;
$$ LANGUAGE plpgsql;
--To view the function
SELECT * FROM call_for_all_gdm()
--------------------------------------------------------------------------------------------------------------------------------
/* Qn. 80. Generate Pie chart to display patient count  with GDM ,Non GDM */
--------------------------------------------------------------------------------------------------------------------------------
SELECT 'GDM' AS category, COUNT(participant_id) AS counts FROM glucose_tests WHERE diagnosed_gdm = 0
UNION
SELECT 'NON GDM' AS category, COUNT(participant_id) AS counts FROM glucose_tests WHERE diagnosed_gdm = 1;
--------------------------------------------------------------------------------------------------------------------------------