import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()
stay_id = 264490

patient_meds_query = client.query("""
	SELECT ie.subject_id, ie.hadm_id, ie.icustay_id, DRUG_NAME_GENERIC as drug
	FROM `physionet-data`.mimiciii_clinical.icustays ie
	LEFT JOIN `physionet-data`.mimiciii_clinical.prescriptions pr
		ON pr.subject_id = ie.subject_id AND pr.hadm_id = ie.hadm_id
		AND pr.startdate BETWEEN (DATETIME_SUB(ie.intime, INTERVAL 6 HOUR)) AND (DATETIME_ADD(ie.intime, INTERVAL 1 DAY))
	WHERE DRUG_NAME_GENERIC is not null AND DRUG_TYPE = 'MAIN' AND ie.icustay_id=
	""" +
	str(stay_id))
patientmeds_df = patient_meds_query.to_dataframe()

patient_labs_query = client.query("""
	SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id
	, min(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END) as ANIONGAP_min
	, max(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END) as ANIONGAP_max
	, min(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_min
	, max(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_max
	, min(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END) as BANDS_min
	, max(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END) as BANDS_max
	, min(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_min
	, max(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_max
	, min(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END) as BILIRUBIN_min
	, max(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END) as BILIRUBIN_max
	, min(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END) as CREATININE_min
	, max(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END) as CREATININE_max
	, min(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END) as CHLORIDE_min
	, max(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END) as CHLORIDE_max
	, min(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END) as GLUCOSE_min
	, max(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END) as GLUCOSE_max
	, min(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END) as HEMATOCRIT_min
	, max(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END) as HEMATOCRIT_max
	, min(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_min
	, max(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_max
	, min(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END) as LACTATE_min
	, max(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END) as LACTATE_max
	, min(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END) as PLATELET_min
	, max(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END) as PLATELET_max
	, min(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END) as POTASSIUM_min
	, max(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END) as POTASSIUM_max
	, min(CASE WHEN label = 'PTT' THEN valuenum ELSE null END) as PTT_min
	, max(CASE WHEN label = 'PTT' THEN valuenum ELSE null END) as PTT_max
	, min(CASE WHEN label = 'INR' THEN valuenum ELSE null END) as INR_min
	, max(CASE WHEN label = 'INR' THEN valuenum ELSE null END) as INR_max
	, min(CASE WHEN label = 'PT' THEN valuenum ELSE null END) as PT_min
	, max(CASE WHEN label = 'PT' THEN valuenum ELSE null END) as PT_max
	, min(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null END) as SODIUM_min
	, max(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null end) as SODIUM_max
	, min(CASE WHEN label = 'BUN' THEN valuenum ELSE null end) as BUN_min
	, max(CASE WHEN label = 'BUN' THEN valuenum ELSE null end) as BUN_max
	, min(CASE WHEN label = 'WBC' THEN valuenum ELSE null end) as WBC_min
	, max(CASE WHEN label = 'WBC' THEN valuenum ELSE null end) as WBC_max

	FROM
	( -- begin query that extracts the data
	SELECT ie.subject_id, ie.hadm_id, ie.icustay_id
	-- here we assign labels to ITEMIDs
	-- this also fuses together multiple ITEMIDs containing the same data
	, CASE
			WHEN itemid = 50868 THEN 'ANION GAP'
			WHEN itemid = 50862 THEN 'ALBUMIN'
			WHEN itemid = 51144 THEN 'BANDS'
			WHEN itemid = 50882 THEN 'BICARBONATE'
			WHEN itemid = 50885 THEN 'BILIRUBIN'
			WHEN itemid = 50912 THEN 'CREATININE'
			WHEN itemid = 50806 THEN 'CHLORIDE'
			WHEN itemid = 50902 THEN 'CHLORIDE'
			WHEN itemid = 50809 THEN 'GLUCOSE'
			WHEN itemid = 50931 THEN 'GLUCOSE'
			WHEN itemid = 50810 THEN 'HEMATOCRIT'
			WHEN itemid = 51221 THEN 'HEMATOCRIT'
			WHEN itemid = 50811 THEN 'HEMOGLOBIN'
			WHEN itemid = 51222 THEN 'HEMOGLOBIN'
			WHEN itemid = 50813 THEN 'LACTATE'
			WHEN itemid = 51265 THEN 'PLATELET'
			WHEN itemid = 50822 THEN 'POTASSIUM'
			WHEN itemid = 50971 THEN 'POTASSIUM'
			WHEN itemid = 51275 THEN 'PTT'
			WHEN itemid = 51237 THEN 'INR'
			WHEN itemid = 51274 THEN 'PT'
			WHEN itemid = 50824 THEN 'SODIUM'
			WHEN itemid = 50983 THEN 'SODIUM'
			WHEN itemid = 51006 THEN 'BUN'
			WHEN itemid = 51300 THEN 'WBC'
			WHEN itemid = 51301 THEN 'WBC'
		ELSE null
		END AS label
	, -- add in some sanity checks on the values
	-- the where clause below requires all valuenum to be > 0, so these are only upper limit checks
		CASE
		WHEN itemid = 50862 and valuenum >    10 THEN null -- g/dL 'ALBUMIN'
		WHEN itemid = 50868 and valuenum > 10000 THEN null -- mEq/L 'ANION GAP'
		WHEN itemid = 51144 and valuenum <     0 THEN null -- immature band forms, %
		WHEN itemid = 51144 and valuenum >   100 THEN null -- immature band forms, %
		WHEN itemid = 50882 and valuenum > 10000 THEN null -- mEq/L 'BICARBONATE'
		WHEN itemid = 50885 and valuenum >   150 THEN null -- mg/dL 'BILIRUBIN'
		WHEN itemid = 50806 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
		WHEN itemid = 50902 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
		WHEN itemid = 50912 and valuenum >   150 THEN null -- mg/dL 'CREATININE'
		WHEN itemid = 50809 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
		WHEN itemid = 50931 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
		WHEN itemid = 50810 and valuenum >   100 THEN null -- % 'HEMATOCRIT'
		WHEN itemid = 51221 and valuenum >   100 THEN null -- % 'HEMATOCRIT'
		WHEN itemid = 50811 and valuenum >    50 THEN null -- g/dL 'HEMOGLOBIN'
		WHEN itemid = 51222 and valuenum >    50 THEN null -- g/dL 'HEMOGLOBIN'
		WHEN itemid = 50813 and valuenum >    50 THEN null -- mmol/L 'LACTATE'
		WHEN itemid = 51265 and valuenum > 10000 THEN null -- K/uL 'PLATELET'
		WHEN itemid = 50822 and valuenum >    30 THEN null -- mEq/L 'POTASSIUM'
		WHEN itemid = 50971 and valuenum >    30 THEN null -- mEq/L 'POTASSIUM'
		WHEN itemid = 51275 and valuenum >   150 THEN null -- sec 'PTT'
		WHEN itemid = 51237 and valuenum >    50 THEN null -- 'INR'
		WHEN itemid = 51274 and valuenum >   150 THEN null -- sec 'PT'
		WHEN itemid = 50824 and valuenum >   200 THEN null -- mEq/L == mmol/L 'SODIUM'
		WHEN itemid = 50983 and valuenum >   200 THEN null -- mEq/L == mmol/L 'SODIUM'
		WHEN itemid = 51006 and valuenum >   300 THEN null -- 'BUN'
		WHEN itemid = 51300 and valuenum >  1000 THEN null -- 'WBC'
		WHEN itemid = 51301 and valuenum >  1000 THEN null -- 'WBC'
		ELSE le.valuenum
		END AS valuenum

	FROM `physionet-data.mimiciii_clinical.icustays` ie

	LEFT JOIN `physionet-data.mimiciii_clinical.labevents` le
		ON le.subject_id = ie.subject_id AND le.hadm_id = ie.hadm_id
		AND le.charttime BETWEEN (DATETIME_SUB(ie.intime, INTERVAL 6 HOUR)) AND (DATETIME_ADD(ie.intime, INTERVAL 1 DAY))
		AND le.ITEMID in
		(
		-- comment is: LABEL | CATEGORY | FLUID | NUMBER OF ROWS IN LABEVENTS
		50868, -- ANION GAP | CHEMISTRY | BLOOD | 769895
		50862, -- ALBUMIN | CHEMISTRY | BLOOD | 146697
		51144, -- BANDS - hematology
		50882, -- BICARBONATE | CHEMISTRY | BLOOD | 780733
		50885, -- BILIRUBIN, TOTAL | CHEMISTRY | BLOOD | 238277
		50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
		50902, -- CHLORIDE | CHEMISTRY | BLOOD | 795568
		50806, -- CHLORIDE, WHOLE BLOOD | BLOOD GAS | BLOOD | 48187
		50931, -- GLUCOSE | CHEMISTRY | BLOOD | 748981
		50809, -- GLUCOSE | BLOOD GAS | BLOOD | 196734
		51221, -- HEMATOCRIT | HEMATOLOGY | BLOOD | 881846
		50810, -- HEMATOCRIT, CALCULATED | BLOOD GAS | BLOOD | 89715
		51222, -- HEMOGLOBIN | HEMATOLOGY | BLOOD | 752523
		50811, -- HEMOGLOBIN | BLOOD GAS | BLOOD | 89712
		50813, -- LACTATE | BLOOD GAS | BLOOD | 187124
		51265, -- PLATELET COUNT | HEMATOLOGY | BLOOD | 778444
		50971, -- POTASSIUM | CHEMISTRY | BLOOD | 845825
		50822, -- POTASSIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 192946
		51275, -- PTT | HEMATOLOGY | BLOOD | 474937
		51237, -- INR(PT) | HEMATOLOGY | BLOOD | 471183
		51274, -- PT | HEMATOLOGY | BLOOD | 469090
		50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
		50824, -- SODIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 71503
		51006, -- UREA NITROGEN | CHEMISTRY | BLOOD | 791925
		51301, -- WHITE BLOOD CELLS | HEMATOLOGY | BLOOD | 753301
		51300  -- WBC COUNT | HEMATOLOGY | BLOOD | 2371
		)
		AND valuenum IS NOT null AND valuenum > 0 -- lab values cannot be 0 and cannot be negative
	) pvt
	WHERE pvt.icustay_id=""" +
	str(stay_id)
	+ """ GROUP BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id
	ORDER BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id
	""")
patient_labs_df = patient_labs_query.to_dataframe()

print(patient_labs_df)

demog_query = """
SELECT A.SUBJECT_ID, A.ADMISSION_TYPE, A.ADMISSION_LOCATION, B.ICUSTAY_ID, C.GENDER
FROM `physionet-data.mimiciii_clinical.admissions` A
JOIN `physionet-data.mimiciii_clinical.icustays` B
ON A.SUBJECT_ID = B.SUBJECT_ID 
AND A.HADM_ID = B.HADM_ID
AND B.INTIME BETWEEN A.ADMITTIME AND A.DISCHTIME
JOIN `physionet-data.mimiciii_clinical.patients` C
ON A.SUBJECT_ID = C.SUBJECT_ID
WHERE B.icustay_id="""+ str(stay_id)

demog_df = client.query(demog_query).to_dataframe()

print(demog_df)

service_query = """
SELECT A.SUBJECT_ID, A.HADM_ID, A.TRANSFERTIME, A.CURR_SERVICE, B.ICUSTAY_ID FROM `physionet-data.mimiciii_clinical.services` A
JOIN `physionet-data.mimiciii_clinical.icustays` B
ON A.SUBJECT_ID = B.SUBJECT_ID 
AND A.HADM_ID = B.HADM_ID
AND A.TRANSFERTIME BETWEEN B.INTIME AND B.OUTTIME
WHERE B.icustay_id="""+ str(stay_id)

service_df = client.query(service_query).to_dataframe()

print(service_df)

quit()
