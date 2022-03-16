
/*This procedure merges the Medication Files from Various Years
and Combines them with Hospitalization Data*/


cd "Y:\Clozapine_from_CIHI\DSS_DataRequestBPM6939_Datasets"

fs *.sas7bdat 
foreach f in `r(files)' {
import sas using `f', clear
save "`f'.dta", replace
} 
 
 
cd "Y:\Clozapine_from_CIHI\Lloyd_Folder"
//here consolidate the DAD cohort files
fs dad_co*.dta

foreach f in `r(files)' {
append using "`f'"
} 

//here consolidate the DAD post files

fs dad_pos*.dta

foreach f in `r(files)' {
append using "`f'"
save "dad_post_08_18.dta", replace
} 

//here consolidate the NACRS cohort files
clear
fs nacrs_co*.dta

foreach f in `r(files)' {
append using "`f'"
save "nacrs_cohorts_08_18.dta", replace
} 

//here consolidate the NACRS post files
cd "Y:\Clozapine_from_CIHI\Lloyd_Folder\NACRS_POST_FILES"
clear
fs nacrs_pos*.dta

foreach f in `r(files)' {
append using "`f'"
save "nacrs_post_08_18.dta", replace
} 

//here go through the drugs table
//and filter all clozapine users
//by row
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\NPDUIS.dta"

gen drug_code = strtrim(ATC_LEVEL_5_CODE)
gen drug_desc = strtrim(ATC_LEVEL_5_E_DESC)

gen clz = 1 if drug_desc=="CLOZAPINE"
replace clz= 0 if clz==.

//Harmonize variables
//create derived ones

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_cohorts_08_18.dta"

destring(BIRTH), gen(birth_yr)
gen age = year(admission_date)-birth_yr

//create female variable
gen female=0
replace female = 1 if GENDER_CODE=="F"
replace female = 0 if GENDER_CODE=="M" | GENDER_CODE=="O"

//collapse rural/remote with unknown
gen rural_unk = 1 if URBAN_RURAL_REMOTE=="RURAL/REMOTE" | URBAN_RURAL_REMOTE=="UNK"
replace rural_unk = 0 if URBAN_RURAL_REMOTE=="URBAN"

//create identifier for DAD/NACRS
gen source_DAD = 1

//search the diagnostic codes here
//the codes have been modified to conform with the document:
//Y:\Clozapine_from_CIHI\Results\ICD-codes_rev_15_Sept_2020.docx


gen schizoph = 0
 

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
		replace schizoph = 1 if inlist(`var',"F200","F201","F202","F203","F204")
		replace schizoph = 1 if inlist(`var', "F205", "F205","F206","F208", "F209")
}	

gen schizaff = 0
foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace schizaff = 1 if inlist(`var',"F250","F251","F252","F258","F259")
}

gen bipolar = 0
foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace bipolar = 1 if inlist(`var',"F310","F311","F312","F313","F314")
	replace bipolar = 1 if inlist(`var',"F315","F316","F317","F318","F319")
}



gen self_harm=0
foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace self_harm = 1 if inlist(`var',"X60","X61","X62","X63","X64")
	replace self_harm = 1 if inlist(`var',"X65","X66","X67","X68","X69")
	replace self_harm = 1 if inlist(`var',"X70","X71","X72","X73","X74","X75")
	replace self_harm = 1 if inlist(`var',"X76","X77","X78","X79","X80")
	replace self_harm = 1 if inlist(`var',"X81","X82","X83","X84")
}



//Corrected to include I514 (11 Nov 2020)
gen myocarditis = 0

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace myocarditis = 1 if inlist(`var', "I401","I408", "I409", "I41", "I514")
}

//Corrected to include I422 (11 Nov 2020)
gen cardiomyopathy = 0

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace cardiomyopathy = 1 if inlist(`var', "I420", "I421", "I422" "I423" "I424", "I425", "I427", "I428", "I429")
}

gen psychos_non = 0

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace psychos_non = 1 if inlist(`var', "F28")
}

gen psychos_org = 0

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace psychos_org = 1 if inlist(`var', "F29")
}


//create province variable
destring SUBMIT, gen(province)
lab def prob 6"Man" 7"Sask" 9"BC"
lab values province prob

//create year from Fiscal year
gen fiscal_yr = FISC

/*################################
Do the same as above for the NACRS file
################################*/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_cohorts_08_18.dta"

destring(BIRTH), gen(birth_yr)
gen age = year(DATE_OF_REGISTRATION)-birth_yr

//create female variable
gen female=0
replace female = 1 if GENDER_CODE=="F"
replace female = 0 if GENDER_CODE=="M" | GENDER_CODE=="O"

//collapse rural/remote with unknown
gen rural_unk = 1 if URBAN_RURAL_REMOTE=="RURAL/REMOTE" | URBAN_RURAL_REMOTE=="UNK"
replace rural_unk = 0 if URBAN_RURAL_REMOTE=="URBAN"

//create province variable
destring SUBMIT, gen(province)
lab def prob 6"Man" 7"Sask" 9"BC"
lab values province prob


gen schizoph = 0
foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
		replace schizoph = 1 if inlist(`var',"F200","F201","F202","F203","F204")
		replace schizoph = 1 if inlist(`var', "F205", "F205","F206","F208", "F209")
}

gen bipolar = 0
foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{

	replace bipolar = 1 if inlist(`var',"F310","F311","F312","F313","F314")
	replace bipolar = 1 if inlist(`var',"F315","F316","F317","F318","F319")

}

gen schizaff = 0

foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace schizaff = 1 if inlist(`var',"F250","F251","F252","F258","F259")
}


gen self_harm = 0

foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace self_harm = 1 if inlist(`var',"X60","X61","X62","X63","X64")
	replace self_harm = 1 if inlist(`var',"X65","X66","X67","X68","X69")
	replace self_harm = 1 if inlist(`var',"X70","X71","X72","X73","X74","X75")
	replace self_harm = 1 if inlist(`var',"X76","X77","X78","X79","X80")
	replace self_harm = 1 if inlist(`var',"X81","X82","X83","X84")
}

//Corrected to include I514 (11 Nov 2020)
gen myocarditis = 0
foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace myocarditis = 1 if inlist(`var', "I401","I408", "I409", "I41", "I514")
}

//Corrected to include I422 (11 Nov 2020)
gen cardiomyopathy = 0
foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace cardiomyopathy = 1 if inlist(`var', "I420", "I421" "I422" "I423" "I424", "I425", "I427", "I428", "I429")
}

gen psychos_non = 0

foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace psychos_non = 1 if inlist(`var', "F28")
}

gen psychos_org = 0

foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace psychos_org = 1 if inlist(`var', "F29")
}


//create identifier for DAD/NACRS
gen source_DAD = 0


/*
30 Sept 2020
Work on the DAD post file here
*/


use "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_post_08_18.dta"

destring(birthd), gen(birth_yr)
gen age = year(admission_date)-birth_yr

//create female variable
gen female=0
replace female = 1 if GENDER_CODE=="F"
replace female = 0 if GENDER_CODE=="M" | GENDER_CODE=="O"

//collapse rural/remote with unknown
gen rural_unk = 1 if URBAN_RURAL_REMOTE=="RURAL/REMOTE" | URBAN_RURAL_REMOTE=="UNK"
replace rural_unk = 0 if URBAN_RURAL_REMOTE=="URBAN"

//create identifier for DAD/NACRS
gen source_DAD = 1

//search the diagnostic codes here
//the codes have been modified to conform with the document:
//Y:\Clozapine_from_CIHI\Results\ICD-codes_rev_15_Sept_2020.docx

/* for exporting CSV: export delimited mbun DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25 any_mental_dx using "\\cabinet.usask.ca\work$\llb296\My Documents\JuliaLang\dad_08_18_cohorts.csv", replace

*/

gen schizoph = 0
 

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
		replace schizoph = 1 if inlist(`var',"F200","F201","F202","F203","F204")
		replace schizoph = 1 if inlist(`var', "F205", "F205","F206","F208", "F209")
}	

gen schizaff = 0
foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace schizaff = 1 if inlist(`var',"F250","F251","F252","F258","F259")
}

gen bipolar = 0
foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace bipolar = 1 if inlist(`var',"F310","F311","F312","F313","F314")
	replace bipolar = 1 if inlist(`var',"F315","F316","F317","F318","F319")
}



gen self_harm=0
foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace self_harm = 1 if inlist(`var',"X60","X61","X62","X63","X64")
	replace self_harm = 1 if inlist(`var',"X65","X66","X67","X68","X69")
	replace self_harm = 1 if inlist(`var',"X70","X71","X72","X73","X74","X75")
	replace self_harm = 1 if inlist(`var',"X76","X77","X78","X79","X80")
	replace self_harm = 1 if inlist(`var',"X81","X82","X83","X84")
}


//Corrected to include I514 (11 Nov 2020)
gen myocarditis = 0

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace myocarditis = 1 if inlist(`var', "I401","I408", "I409", "I41", "I514")
}

//Corrected to include I422 (11 Nov 2020)
gen cardiomyopathy = 0

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace cardiomyopathy = 1 if inlist(`var', "I420", "I421", "I422" "I423" "I424", "I425", "I427", "I428", "I429")
}

gen psychos_non = 0

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace psychos_non = 1 if inlist(`var', "F28")
}

gen psychos_org = 0

foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
	replace psychos_org = 1 if inlist(`var', "F29")
}


//create province variable
destring SUBMIT, gen(province)
lab def prob 6"Man" 7"Sask" 9"BC"
lab values province prob

//create year from Fiscal year
gen fiscal_yr = FISC


/*
30 Sept 2020
This syntax is for NACRS post
*/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_post_08_18.dta"
destring(birthd), gen(birth_yr)
gen age = year(DATE_OF_REGISTRATION)-birth_yr

//create female variable
gen female=0
replace female = 1 if GENDER_CODE=="F"
replace female = 0 if GENDER_CODE=="M" | GENDER_CODE=="O"

//collapse rural/remote with unknown
gen rural_unk = 1 if URBAN_RURAL_REMOTE=="RURAL/REMOTE" | URBAN_RURAL_REMOTE=="UNK"
replace rural_unk = 0 if URBAN_RURAL_REMOTE=="URBAN"

//create province variable
destring SUBMIT, gen(province)
lab def prob 6"Man" 7"Sask" 9"BC"
lab values province prob


gen schizoph = 0
foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
		replace schizoph = 1 if inlist(`var',"F200","F201","F202","F203","F204")
		replace schizoph = 1 if inlist(`var', "F205", "F205","F206","F208", "F209")
}

gen bipolar = 0
foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{

	replace bipolar = 1 if inlist(`var',"F310","F311","F312","F313","F314")
	replace bipolar = 1 if inlist(`var',"F315","F316","F317","F318","F319")

}

gen schizaff = 0

foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace schizaff = 1 if inlist(`var',"F250","F251","F252","F258","F259")
}


gen self_harm = 0

foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace self_harm = 1 if inlist(`var',"X60","X61","X62","X63","X64")
	replace self_harm = 1 if inlist(`var',"X65","X66","X67","X68","X69")
	replace self_harm = 1 if inlist(`var',"X70","X71","X72","X73","X74","X75")
	replace self_harm = 1 if inlist(`var',"X76","X77","X78","X79","X80")
	replace self_harm = 1 if inlist(`var',"X81","X82","X83","X84")
}

//Corrected to include I514 (11 Nov 2020)
gen myocarditis = 0
foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace myocarditis = 1 if inlist(`var', "I401","I408", "I409", "I41", "I514")
}

//Corrected to include I422 (11 Nov 2020)
gen cardiomyopathy = 0
foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace cardiomyopathy = 1 if inlist(`var', "I420", "I421" "I422" "I423" "I424", "I425", "I427", "I428", "I429")
}

gen psychos_non = 0

foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace psychos_non = 1 if inlist(`var', "F28")
}

gen psychos_org = 0

foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
	replace psychos_org = 1 if inlist(`var', "F29")
}


//create identifier for DAD/NACRS
gen source_DAD = 0





/*################################
Here we create DAD and NACRS files
with minimal variables 
These will be appended to Aggregate_Records
################################*/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_cohorts_08_18.dta"

ren FISCAL_YEAR fiscal_yr
/*
 Here is the code for
 regexm to filter the mental health diagnoses in the follow ups
*/

//fix the any_mental_dx variable in dad_cohorts_08_18.dta and aggregate records
foreach var of varlist DIAG_CODE_1 DIAG_CODE_2 DIAG_CODE_3 DIAG_CODE_4 DIAG_CODE_5 DIAG_CODE_6 DIAG_CODE_7 DIAG_CODE_8 DIAG_CODE_9 ///
DIAG_CODE_10 DIAG_CODE_11 DIAG_CODE_12 DIAG_CODE_13 DIAG_CODE_14 DIAG_CODE_15 DIAG_CODE_16 DIAG_CODE_17 DIAG_CODE_18 DIAG_CODE_19 DIAG_CODE_20 DIAG_CODE_21 DIAG_CODE_22 DIAG_CODE_23 DIAG_CODE_24 DIAG_CODE_25{
gen any`var'temp = regexm(`var', "^F[0-9]+")
}
egen any_mental_dx = rmax(anyDIAG_CODE_1temp- anyDIAG_CODE_25temp)

//updated 11 Nov 2020
keep mbun province birth_yr age female rural_unk source_DAD self_harm psychos_non psychos_org schizoph schizaff bipolar any_mental_dx fiscal_yr episode_beg_dt episode_end_dt myocarditis cardiomyopathy


/*keep mbun province fiscal_yr birth_yr age female admission_date discharge_date rural_unk source_DAD schizoph schizaff bipolar self_harm myocarditis psychos_non psychos_org any_mental_dx drug_code drug_desc clz
 save "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records.dta"
 
ren admission_date episode_beg_dt
ren discharge_date episode_end_dt
*/

gen drug_code = .
gen drug_desc = .
gen clz = . 

save "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_cohorts_08_18_for_append.dta"

/*ren DATE_OF_REGISTRATION episode_beg_dt
ren DISPOSITION_DATE episode_end_dt
*/


use "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_cohorts_08_18.dta"
gen drug_code = .
gen drug_desc = .
gen clz = . 


keep mbun province birth_yr age female rural_unk source_DAD self_harm psychos_non psychos_org schizoph schizaff bipolar any_mental_dx fiscal_yr episode_beg_dt episode_end_dt myocarditis cardiomyopathy  drug_code drug_desc clz
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_cohorts_08_18_for_append.dta"



foreach var of varlist ED_DISCHARGE_DIAG_CODE_1 ED_DISCHARGE_DIAG_CODE_2 ED_DISCHARGE_DIAG_CODE_3 MAIN_PROBLEM OTHER_PROBLEM_1 OTHER_PROBLEM_2 /// 
 OTHER_PROBLEM_3 OTHER_PROBLEM_4 OTHER_PROBLEM_5 OTHER_PROBLEM_6 OTHER_PROBLEM_7 OTHER_PROBLEM_8 OTHER_PROBLEM_9{
  gen any`var'temp = regexm(`var', "^F[0-9]+")
 }
   
egen any_mental_dx = rmax(anyED_DISCHARGE_DIAG_CODE_1temp-anyOTHER_PROBLEM_9temp)
 
keep mbun case_id province fiscal_yr birth_yr age female episode_beg_dt episode_end_dt rural_unk source_DAD schizoph schizaff bipolar self_harm myocarditis psychos_non psychos_org any_mental_dx drug_code drug_desc clz


/****
This procedure is for appending the DAD, NACRS Cohort and Post files

****/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_post_08_18_for_append.dta"






/****
End of Procedure
****/








use "Y:\Clozapine_from_CIHI\Lloyd_Folder\NPDUIS_for_append.dta"

ren Juris province
ren MBUN mbun
ren Service episode_beg_dt
gen episode_end_dt = episode_beg_dt
replace episode_beg_dt = .

gen case_id = .
gen birth_yr = .
gen age = .
gen female = .
gen rural_unk = .
gen source_DAD = .
gen schizoph = .
gen schizaff = .
gen bipolar = .
gen self_harm = .
gen myocarditis = .
gen psychos_non = .
gen psychos_org = .
gen any_mental_dx = .
gen source_DAD = -1
gen fiscal_yr = year(episode_beg_dt)

/*Now work with the
Aggregate_Records
file
*/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records.dta"

sort mbun episode_beg_dt

//fix the episode_beg_dt such that it becomes the same as episode_end_dt
//for the NPDUIS records

replace episode_beg_dt = episode_end_dt if source_DAD==-1

********************************CREATE INDEX HOSPITALIZATION****************************************
//create an index variable for the first hospitalization per patient
//the next line sorts by patient date and hospital events first
//do this by creating a temp file of NACRS/DAD only and then create a variable called
//index_hosp there. 


/***Error***The index hospitalization should include hospitalizations within 3 daysafter
Also make sure that Aggregate_Records = dad_08_18_cohorts + dad_post_08_18
+ nacrs_cohorts_08_18 + nacrs_post_08_18 + NPDUIS */

//the dataset in the next line is empty
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records_working_10_10_20.dta"

append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\NPDUIS.dta", keep(mbun province fiscal_yr episode_beg_dt episode_end_dt clz drug_code drug_desc source_DAD) force
compress
save, replace
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_cohorts_08_18.dta", keep(mbun province fiscal_yr birth_yr episode_beg_dt episode_end_dt age female rural_unk source_DAD self_harm myocarditis psychos_non psychos_org schizoph schizaff bipolar any_mental_dx) force
compress
save, replace
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_post_08_18.dta", keep(mbun province fiscal_yr birth_yr episode_beg_dt episode_end_dt age female rural_unk source_DAD self_harm myocarditis psychos_non psychos_org schizoph schizaff bipolar any_mental_dx) force
compress
save, replace

append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_cohorts_08_18.dta", keep(mbun province fiscal_yr birth_yr episode_beg_dt episode_end_dt age female rural_unk source_DAD self_harm myocarditis psychos_non psychos_org schizoph schizaff bipolar any_mental_dx) force
compress
save, replace
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_post_08_18.dta", keep(mbun province fiscal_yr birth_yr episode_beg_dt episode_end_dt age female rural_unk source_DAD self_harm myocarditis psychos_non psychos_org schizoph schizaff bipolar any_mental_dx) force
compress
save, replace

/******************************************************************************************************************
This is a hospitalizations only file (no NPDUIS)
First we have to merge cardiomyopathy from the raw files
*******************************************************************************************************************/
//use "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalizations_file.dta"

**first combine DAD cohorts and post
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_cohorts_08_18.dta"
keep mbun episode_beg_dt episode_end_dt cardiomyopathy
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_cohorts_cardiomyo_temp.dta"

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_post_08_18.dta"
keep mbun episode_beg_dt episode_end_dt cardiomyopathy
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_post_cardiomyo_temp.dta"

append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_cohorts_cardiomyo_temp.dta"

**Now combine NACRS cohorts and post
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_cohorts_08_18.dta"
keep mbun episode_beg_dt episode_end_dt cardiomyopathy
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_cohorts_cardiomyo_temp.dta"


use "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_post_08_18.dta"
keep mbun episode_beg_dt episode_end_dt cardiomyopathy
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_post_cardiomyo_temp.dta"

append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\nacrs_cohorts_cardiomyo_temp.dta"

****Use the hospitalizations file and merge the nacrs & dad cardiomyo temp file
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalizations_file.dta"
merge m:m mbun episode_beg_dt episode_end_dt using "Y:\Clozapine_from_CIHI\Lloyd_Folder\dad_nacrs_cmyo_for_merge.dta", keepusing(cardiomyopathy)

save, replace
order mbun- any_mental_dx cardiomyopathy
******************Now group together the hospitalizations that are within 3 days***************************
***********************************************************************************************************

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalizations_file_copy.dta"

bysort mbun (episode_beg_dt): gen state = 0 if index_hosp==1
//this takes care of records immediately following index hosp 
//handles both singletons and grouped records
bysort mbun (episode_beg_dt): replace state = 1 if subeq_hosp==1 & index_hosp[_n-1]==1

//take care of singleton records that DO NOT IMMEDIATELY following index hospitalization
bysort mbun (episode_beg_dt):replace state = 1 if subeq_hosp==1 & group==.

//take care of records that are subsequent hosps that are part of a group
bysort mbun (episode_beg_dt): replace state = 1 if subeq_hosp==1 & group==1 & group[_n-]==.

//take care of records that are the first in a GROUP of subsequent hosp
bysort mbun (episode_beg_dt):replace state = 1 if subeq_hosp==1 & group==0


//check that the only remaining missing cases for state are those that are part of a group
list mbun if state==. & group!=1

//now we can replace those as 0, i.e. hosp episodes that are part of a group
bysort mbun (episode_beg_dt):replace state = 0 if state==. & group==1

//Now create a numbered count of hospitalizations
bysort mbun (episode_beg_dt): gen rehosp_ctr = sum(state)



***************Now create an index hospitalization date

****do this by aggregating the dad nacrs files only (4 files) and then tagging the first entry but also those 
****that are within 3 days of the first entry
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records_working_10_10_20.dta"
keep if source>-1
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\temp.dta", replace


//this one groups the index hospitalizations
sort mbun episode_beg_dt
bysort mbun (episode_beg_dt): gen index_hosp = 1 if _n==1
bysort mbun (episode_beg_dt): replace index_hosp = 1 if episode_beg_dt-episode_end_dt[1] <=3 & index_hosp==.


//this one groups the subsequent hospitalizations

bysort mbun (episode_beg_dt): gen subeq_hosp = 1 if _n>1 & index_hosp==.

//the last condition ensures that the first subsequent hospitalization is excluded
bysort mbun (episode_beg_dt): gen group =1 if subeq_hosp==1 & episode_beg_dt-episode_end_dt[_n-1] <=3 & index_hosp[_n-1]!=1

//now replace the first record of a subsequent hospitalization that should be part of the group
//don't use index hospitalization as an 
bysort mbun (episode_beg_dt): replace group = 1 if subeq_hosp==1 & subeq_hosp[_n+1]==1 & (episode_beg_dt[_n+1] - episode_end_dt <=3)
//make sure that the records within a group are truly not within 3 days
bysort mbun (episode_beg_dt): replace group = 0 if subeq_hosp==1 & group==1 & episode_beg_dt-episode_end_dt[_n-1]>3




/***********************************DELETE THE DRUG RECORDS THAT PRECEDE FIRST HOSP*******************
******************************************************************************************************/
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records_working_Nov_03_20.dta"
merge m:m mbun episode_beg_dt episode_end_dt using "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalizations_file_copy.dta", keepusing(cardiomyopathy index_hosp rehosp_ctr) force

//the next line lists the index hospitalization first, followed by 0' and missing
gsort mbun -index_hosp
by mbun: gen to_drop = 1 if source_DAD==-1 & episode_beg_dt[_n] < episode_beg_dt[1]

//save the record of meds before index: meds_before_index_hosp_10_14.dta

drop if to_drop==1
drop to_drop
compress
save, replace

*************note that the Aggregate Records with prior meds file is still avaialable*******************
**********************************meds_before_index_hosp_10_14.dta**************************************




























***********************************CREATE A FILE of PATIENTS THAT WERE UNTREATED*******************
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records_with_meds_before_index_hosp.dta"
keep mbun episode_beg_dt episode_end_dt to_drop
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\meds_before_index_hosp.dta"

//Now open the temp drugs file
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\NPDUIS.dta"
gen episode_beg_dt = Service_Date
ren MBUN mbun
merge m:m mbun episode_beg_dt using "Y:\Clozapine_from_CIHI\Lloyd_Folder\meds_before_index_hosp.dta", keepusing(to_drop)
keep if to_dr==.

gen took_drugs_post_hosp_idx=1
keep mbun episode_beg_dt took_drugs_post_hosp_idx
compress
//keep only unique patients
bysort mbun(episode_beg_dt): keep if _n==1
//the following file contains only patients that were
//treated after index hospitalization
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\unique_patients_treated.dta"

/*********************************************************************************************





********************************************Now mergee the Aggregate_Records with Took drugs****/
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records.dta"
drop to_drop
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\unique_patients_treated.dta", keepusing(took_drugs_post_hosp_idx)


keep if took==1
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records.dta", replace
****we may have to handle 'multiple' first drugs
//populate the variables case_id, fiscal_yr, birth_yr, age, female, rural
//create a pre-hospitalization drug variable????  No because we cannot 
//make the diagnosis retroactive to the hospital admission_date
//Also, follow Joas by having the exposure period coincide with the drug started after the index hospitalization
//Restrict the drugs to those that were dispensed and then refilled after 6 weeks.
//In other words, exclude the patients who did not have at least 1 6-week dispense/refill  --> this is in order to exclude the untreated as Joas did.



***see the looping procedure here
//https://www.stata.com/statalist/archive/2006-11/msg00103.html


*********************Unique subjects who took meds after an index psych hospitalization****************************************************

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records.dta"
bysort mbun: keep if _n==1
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\unique_subj_Agg_Rec.dta"

*******************unique N: 47,554**** mean age: 43.39***********pct male: 52.38**********************************

********************************************Populate the unchanging variables in the drug records************************************************
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records.dta"

replace fiscal_yr = year(episode_beg_dt) if fiscal_yr==.
bysort mbun (episode_beg_dt): replace birth_yr = birth_yr[1] if birth_yr==.
bysort mbun (episode_beg_dt): replace female = female[1] if female==.
bysort mbun (episode_beg_dt): replace rural_unk = rural_unk[1] if rural_unk==.
replace age = fiscal_yr-birth_yr if age==.

/****************************************Create dummy variables for major antipsychotics************************************************
**************************************Here, follow the 10 atypical antipsychotics listed in url below plus lithium (confounder only) https://www.canada.ca/en/health-canada/services/drugs-health-products/medeffect-canada/safety-reviews/atypical-antipsychotics-assessing-potential-risk-drug-reaction-eosinophilia-systemic-symptoms.html
*/
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records.dta"
gen arip = 1 if drug_desc=="ARIPIPRAZOLE"
gen olan = 1 if drug_desc=="OLANZAPINE"
gen pali = 1 if drug_desc=="PALIPERIDONE"
gen quet = 1 if drug_desc=="QUETIAPINE"
gen lith = 1 if drug_desc=="LITHIUM"
gen risp = 1 if drug_desc=="RISPERIDONE"
gen zip = 1 if drug_desc=="ZIPRASIDONE"


****************************************Create file for selected antipsychotics only************************************************
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records.dta"

egen select_drugs = rmax(clz arip olan pali quet lith risp zip)
drop if select_drugs==0 & source_DAD==-1

save " Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records_Select_Antipsych.dta"
//now we may have to create hosp spells and drug spells

****************************************Create state variable***********************************************************************
//all hospitalizations are state 1 except initial
bysort mbun (episode_beg_dt): gen state = 1 if source_DAD>-1 & index_hosp==.
replace state = 2 if source_DAD==-1 
replace state = 0 if index_hosp==1

lab def anoba 0"index hosp" 1"subseq hosp" 2"drug fill"
lab values state anoba

//tag then delete duplicated drugs
bysort mbun (episode_beg_dt): gen dup_drug = 1 if source_DAD==-1 & source_DAD[_n-1]==-1 & episode_beg_dt==episode_beg_dt[_n-1] & drug_code==drug_code[_n-1]
drop if dup_drug==1

bysort mbun (episode_beg_dt drug_code): replace dup_drug=1 if source_DAD==-1 & source_DAD[_n-1]==-1 & episode_beg_dt==episode_beg_dt[_n-1] & drug_code==drug_code[_n-1]


***important: note that the "subsequent hospitalization" state need to be broken into separate spells if they are not of the same date
/*Let's do this by creating a state change marker  (1/0) ********************************************************************************
*/
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records_Select_Antipsych.dta"
bysort mbun (episode_beg_dt): gen state_chg = cond(state != state[_n-1], 1, 0)

//Now take the idea one state further with a substate
//restrict the marker to the subsequent hospitalizations only
//and not the beginning record
bysort mbun (episode_beg_dt): gen substate_chg = cond(state==1 &  state_chg==0 & episode_beg_dt-episode_end_dt[_n-1]>3, 1, 0)


//need to group index & subseq hospits that are within 3 days


***need to clarify with subject expert if psych meds are continued in the hospital if confined for a non-psych reason--Jenna says yes


************************This procedure is for correcting the index_hosp variable in the file hospitalization_rehosp_3_Nov.dta**************************************
***********************It will also correct the myocarditis variable so that I54 will be included *****************************************************************
/* November 11, 2020 */


use "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalization_rehosp_3_Nov.dta"

ren episode_beg_dt tmp_episode_beg_dt
ren episode_end_dt tmp_episode_end_dt

gen episode_beg_dt = date(tmp_episode_beg_dt, "DMY")
format episode_beg_dt %td

gen episode_end_dt = date(tmp_episode_end_dt, "DMY")
format episode_end_dt %td

sort mbun episode_beg_dt episode_end_dt

gen corr_index_hosp = 1 if rehosp_ctr==0
replace corr_index_hosp = 0 if rehosp_ctr>0 & rehosp_ctr <.


save "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalization_rehosp_11_Nov.dta"


************************This procedure is for preparing the NPDUIS file for appending******************************************************************************
***********************It will also correct the myocarditis variable so that I54 will be included *****************************************************************
/* November 13, 2020 */

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\NPDUIS.dta"

gen int probinsya = .
replace probinsya = 6 if province=="MB"
replace probinsya = 7 if province=="SK"
replace probinsya = 9 if province=="BC"


destring SUBMIT, gen(province)
lab def prob 6"Man" 7"Sask" 9"BC"
lab values probinsya prob
drop province

ren probi province
keep mbun province fiscal_yr drug_code drug_desc episode_beg_dt episode_end_dt clz source_DAD

gen birth_yr = .
gen age = .
gen byte female =. 
gen byte rural_unk =. 
gen byte self_harm =. 
gen bytepsychos_non =. 
gen byte psychos_org =. 
gen byte schizoph =. 
gen byte schizaff =. 
gen byte bipolar =. 
gen byte any_mental_dx =. 
gen byte myocarditis =. 
gen byte cardiomyopathy=. 
gen byte index_hosp = .
gen int rehosp_ctr = .

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalization_rehosp_12_Nov.dta"
tostring drug_code, replace
tostring drug_desc, replace

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\NPDUIS.dta"
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalization_rehosp_12_Nov.dta"
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records_working_Nov_13_20.dta"

sort mbun episode_beg_dt episode_end_dt
order mbun province birth_yr age female rural_unk fiscal_yr source_DAD schizoph schizaff bipolar psychos_org psychos_non any_mental_dx self_harm myocarditis cardiomyopathy episode_beg_dt episode_end_dt

bysort mbun (birth_yr): replace birth_yr= birth_yr[1] if birth_yr==.
bysort mbun (female): replace female= female[1] if female==.
bysort mbun (rural_unk): replace rural_unk= rural_unk[1] if rural_unk==.
bysort mbun (day0): replace day0= day0[1] if day0==.



replace age = fiscal_yr-birth_yr if age==.
sort mbun episode_beg_dt episode_end_dt

/************************************************************************************************************************************
Create a day zero variable to indicate the "Big Bang"
************************************************************************************************************************************/
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records_working_Nov_13_20.dta"
drop if source >-1

//create the day zero variable in the hospitalizations_file
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalization_rehosp_12_Nov.dta"

bysort mbun (day0): replace day0= day0[1] if day0==.

sort mbun episode_beg_dt episode_end_dt


/************************************************************************************************************************************
Create a last day of "index_hosp" day1 var
************************************************************************************************************************************/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\hospitalization_rehosp_12_Nov.dta"

bysort mbun (episode_beg_dt episode_end_dt rehosp_ctr): gen day1 = episode_end_dt[_n-1] if rehosp_ctr==1 & index_hosp[_n-1]==1

ren index_date index_hosp

bysort mbun (day1): replace day1= day1[1] if day1==.
bysort mbun (episode_beg_dt episode_end_dt rehosp_ctr): replace day1 = episode_end_dt[1] if day1==. & rehosp_ctr==0

/************************************************************************************************************************************
Deliverable 6 Task 1
For every patient, delete those rows (medications) that were dispensed prior to the first day of the index_hosp.
************************************************************************************************************************************/
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Records_working_Nov_13_20.dta"

bysort mbun (episode_beg_dt episode_end_dt): drop if source_DAD==-1 & episode_beg_dt < day0

/************************************************************************************************************************************
Deliverable 6 Task 2
The next step is to delete the patients (and all their associated rows) who did not receive any antipsychotic medication.  
************************************************************************************************************************************/

drop if source_DAD==-1 & episode_beg_dt <= day1

bysort mbun (episode_beg_dt episode_end_dt): gen post_idx_med = 1 if source_DAD==-1 & episode_beg_dt > day1
bysort mbun (episode_beg_dt episode_end_dt): egen med_rows = sum(post_idx_med)


/**********************************************
The most up-to-date version of the hospitalizations and drugs is
Aggregate_Recs_No_Pre_index_No_Untx_16_Nov_2020.dta"
***********************************************/

//Reconcile the numbers here with Arash Dec 15 file.
 use "Y:\Clozapine_from_CIHI\Lloyd_Folder\from_arash_15_jan_2021_unique_PHD_delv_6_3.dta"
merge m:m mbun episode_beg_dt episode_end_dt using "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Recs_No_Pre_index_No_Untx_16_Nov_2020.dta", keepusing(mbun episode_beg_dt episode_end_dt drug_code drug_desc) force

//The reason for the discrepancy is that medication records between day0 and day1 and are included by Arash.
//The erroneous records are in: "Y:\Clozapine_from_CIHI\Lloyd_Folder\from_arash+15_jan_2021_to_delete.dta"


/**********************************************
*Here we perform Rohit's suggestion of 30-days as
*a threshold for a gap in treatment
***********************************************/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Aggregate_Recs_No_Pre_index_No_Untx_16_Nov_2020.dta"
keep if source_DAD==-1

//delete unnecessary cols
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Clz_medication_gap_calc.dta"
drop source schizaff bipolar psychos_org psychos_non any_mental_dx self_harm myocarditis cardiomyopathy post_idx_med med_rows untreated schizoph
drop index_hosp rehosp_ctr
save, replace

sort mbun episode_end_dt

//number the records for each person
by mbun: gen obs = _n

//create groups by drug and episode date
bysort mbun drug_code episode_end_dt(obs): replace obs = obs[1]


//delete duplicated meds issued on same day
bysort mbun drug_code obs: keep if _n==1

//the next line flags unique drugs
bysort mbun drug_code: gen group = _n==1

//the next line is a counter that
//increments group for every new drug
bysort mbun: replace group = sum(group)

//persist means the previous rec and the current rec are within 30 days
bysort mbun group (drug_code episode_end_dt): gen persist = 1 if episode_end_dt[_n]-episode_end_dt[_n-1]<=30

/*Create subgroup within group
This would indicate spells / consecutive refills
for a particular drug_code 
*/

bysort mbun group (drug_code episode_end_dt): gen subgrp = 1 if episode_end_dt[_n+1]-episode_end_dt[_n]<=30

//Carryover the persist indicator for the beginning of spells
bysort mbun group: replace subgrp=1 if persist==1 & subgrp==.

/*Need to create a duplicate record to serve as transition 
between consecutive refill spells
NOT consecutive refills. This is accomplished by the variable "gap" */

bysort mbun group (episode_end_dt) subgrp: gen gap = 1 if episode_end_dt[_n+1] - episode_end_dt[_n] > 30 & episode_end_dt[_n+1] <.


/*Create a "gap" only for contiguous refill spells WITHIN a particular drug
Contiguous spells associated with different drugs do not need a "gap"*/

expand 2 if gap==1 & persist==1 & subgrp < . & gap[_n+1]==., gen(dup_to_miss_subgrp)
//In the duplicate record, indicate that the record is not to be counted as
//part of the next spell
replace subgrp=0 if dup_to_miss_subgrp==1

//the duplicate records appear at the end of the dataset so 
//insert them at the proper place
sort mbun drug_code group episode_end_dt dup_to_miss_subgrp

//Now create a variable that indicates consecutive 
//values for a specific drug

//create an indicator for consecutive use
gen consec = persist

replace consec = 1 if consec==. & subgrp==1
replace consec = . if subgrp == 0 & consec==1

gen arip = 1 if drug_desc=="ARIPIPRAZOLE" 
gen olan = 1 if drug_desc=="OLANZAPINE"
gen pali = 1 if drug_desc=="PALIPERIDONE"
gen quet = 1 if drug_desc=="QUETIAPINE"
gen lith = 1 if drug_desc=="LITHIUM"
gen risp = 1 if drug_desc=="RISPERIDONE"
gen zip = 1 if drug_desc=="ZIPRASIDONE"
gen hal = 1 if drug_desc=="HALOPERIDOL"


//Now we need to adjust the episode end date for those records with gaps
//Ignore the dup_to_miss variable

bysort mbun group (episode_beg_dt): replace episode_end_dt= episode_end_dt+30 if gap==1
bysort mbun group (episode_beg_dt): replace episode_end_dt= episode_beg_dt[_n+1] if consec[_n]==1 & consec[_n+1]==1


//handle the last records in a group
//do this at the mbun level 

bysort mbun (group episode_beg_dt): replace episode_end_dt= episode_end_dt+30 if ((group[_n+1]-group[_n]>0) & group[_n+1] <.)
//adjust the end date of the very last entry for each person
bysort mbun (group episode_beg_dt): replace episode_end_dt = episode_end_dt+30 if _n==_N


//Now drop the dup_to_miss variable
drop if dup_to_miss==1


//**********************************************
*Here we create a medications file for merging
*back to the hospitalization data
***********************************************/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Clz_medication_gap_calc.dta"

//export this and ask Arash to verify
export delimited using "Y:\Clozapine_from_CIHI\for_Arash\Delv7\Clz_meds_exposure_calculation.csv", replace


***17 March 2021***

/*******************
Create a file of patients who were either on CLOZAPINE or QUETIAPINE only
keeping their full drug history*/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Quet_Clz_patients_full_drug_recs.dta"

/*Narrow down this group of people to those who were given QUETIAPINE within one month of 
their index hospitalization*/
sort  mbun episode_beg_dt
gen qtp=1 if drug_desc=="QUETIAPINE"

gen quet_within_30days_day1 = 1 if qtp==1 & episode_beg_dt - day1 <=31

//The next file contains the mbuns of people who were first given QTP
*save "Y:\Clozapine_from_CIHI\Lloyd_Folder\mbuns_quetiapine_right_after_disch.dta"

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Quet_Clz_patients_full_drug_recs.dta"

//The next file are patients who were initially on Quet after disch, some of whom were switched to clz//
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Quet_inital_opt_Clz_patients_full_drug_recs.dta"


//Further cutdown the patients who were on Quet initially (1st month but who had other meds)
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Quet_inital_opt_Clz_patients_full_drug_recs.dta"

bysort mbun (episode_beg_dt): gen first_31_days = 1 if episode_beg_dt-day1 <=31
bysort mbun (episode_beg_dt): replace first_31_days  = 0 if episode_beg_dt-day1 >30 & episode_beg_dt-day1 <.


//The following file contains the meds in the first 30 days
//We have to exclude the people who had other drugs than QTP in the first 30 days
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\first_31_days_records_subset_QTP_first_CLZ_opt.dta"

keep if drug_desc!="QUETIAPINE"
by mbun: keep if _n==1

//The people in the file next line will now be deleted because they were also on other drugs
*save "Y:\Clozapine_from_CIHI\Lloyd_Folder\mbuns_with_other_meds_first_31_days.dta"

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\Quet_inital_opt_Clz_patients_full_drug_recs.dta"


***18 March 2021***

/*******************
Generalize the procedure above but retain only the 
patients who were on atypical antipsychotics
keeping their full drug history*/

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_All_Drugs_Treated_Patients.dta"
sort mbun episode_beg_dt
by mbun: keep if _n==1

*this is the cohort of unique patients (n=47,584)
*from this file we will see which ones are on atypicals
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_All_Drugs_Treated_Patients_uniq.dta"

*Exclude:
*People already on CLZ, this can be found in 
//Patients_on_CLZ_after_index_hosp.dta

*People already on polypharm in the first 30 days ofter discharge from index hospitalization
*1 Medication records in the first 30 days. This is in:
*CIHI_All_Drugs_Treated_Patients_first_31_days.dta

*2 List of people with polypharmacy in the first_31_days
use "CIHI_All_Drugs_Treated_Patients_first_31_days.dta"
bysort mbun (group): gen max_init_count = group[_N]
by mbun: keep if max_init>1

//the patients with polypharm are in: Patients_with_polypharm_initially.dta (n=22,188)


/*Include those on these second generation antipsychotics:
risperidone (Risperdal)
quetiapine (Seroquel)
olanzapine (Zyprexa)
ziprasidone (Zeldox
paliperidone (Invega)
aripiprazole (Abilify)*/ 

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_All_Drugs_Treated_Patients_uniq.dta"
keep if inlist(drug_desc, "RISPERIDONE","QUETIAPINE", "OLANZAPINE", "ZIPRASIDONE", "PALIPERIDONE", "ARIPIPRAZOLE")

//this results in 35,112 unique people
//in the file Patients_on_SGA_after_index_hosp.dta


/*Now use the CIHI_All_Drugs_Treated_Patients
Retain only the overlap with Patients_on_SGA_after_index_hosp
Remove the Patients with Patients_with_polypharm_initially*/
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_All_Drugs_Treated_Patients.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\Patients_on_SGA_after_index_hosp.dta"
keep if _m==3
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\temp.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\Patients_with_polypharm_initially.dta"
keep if _m==1
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_monophar_long.dta"
//still need to remove the CLZ patients who ever had CLZ as a first med after discharge
*Patients_on_CLZ_as_first_medication_ever.dta

//Now save a unique_patients version (n=19,434)

save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_monophar_uniq.dta" 


/*Now work with the hospitalization_rehosp_12_Nov file 
and retain only the records from CIHI_SGA_monophar_long.dta 
this file is: hospitalizations_of_SGA_cohort.dta
*/
*Now restore the other columns of the SGA file
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_monophar_long.dta"
gen source_DAD=-1
gen psychos_non=. 
gen psychos_org=. 
gen schizoph=. 
gen schizaff=. 
gen bipolar=. 
gen any_mental_dx=. 
gen myocarditis=. 
gen cardiomyopathy=. 
gen rehosp_ctr=. 
gen index_hosp=. 
gen self_harm=.
drop obs group persist subgrp gap dup_to_miss_subgrp consec

save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_monophar_long_for_append_to_hosp.dta

//Now append SGA file and hospitalization file
use " Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_monophar_long_for_append_to_hosp.dta"

//Now here is the file with combined drugs and hosps: 
//Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_and_hospit_18_Mar_2021.dta

sort mbun episode_beg_dt

*******************************
//Here build a cohort of schizophrenia only
//that did not present for self-harm at index
*******************************
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\hospitalizations_of_SGA_cohort.dta"
keep if schizoph==1
keep if index_hosp==1
bysort mbun: keep if _n==1
*result: Schizoph_patients_at_day0.dta 
//Now have a list of self_harm patients
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\hospitalizations_of_SGA_cohort.dta"
keep if self_harm==1
keep if index_hosp==1
bysort mbun: keep if _n==1
*result: Self_harm_patients_at_day0.dta

//The schizophrenia only cohort hospitalizations is: "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\hospitalizations_of_SGA_cohort_SCZ_only.dta"
//The file that excludes self-harmers are: hospitalizations_of_SGA_cohort_SCZ_only_no_SH_day0.dta

//Now limit the Drugs file to only the SCZ non-sh at day 0
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_monophar_long_for_append_to_hosp.dta"

//here is the file that is the SCZ subset
* "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_and_hospit_SCZ_no_SH_18_Mar_2021.dta"

*19 March 2021
/*********Here we will re-do the gap calculation with objectives:
1 create a gap record
2 create polypharmacy indicat
3 Limit the CLZ med gap calculation file to only those in the CIHI_SGA_and_hospit_SCZ_no_SH_18_Mar_2021.dta
*/

//Do #3 step
//Create a set of unique patients in this cohort for filtering Med Gap Calculation file
(n = 7,556)

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_and_hospit_SCZ_no_SH_18_Mar_2021.dta"
bysort mbun: keep if _n==1
keep mbun
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_and_hospit_SCZ_no_SH_18_Mar_2021_uniq.dta", replace


***Re-do everything from the Big All medicines file***

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_med_gap_calc_19_Mar_21.dta"
//create a list of unique people 
bysort mbun (episode_beg_dt): keep if _n==1
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\hospitalizations_of_SGA_cohort.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_uniq_6_Apr_2021.dta"
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_hospitalizations_6_April_2021.dta"
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_hospitalizations_6_April_2021.dta"

//drop the index hospitalizations
drop if index_hosp==1
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_post_idx_hosps_6_April_2021.dta"

//keep only the first rehospitalization
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_post_idx_hosps_6_April_2021.dta"
keep if rehosp_ctr==1
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_post_idx_first_hosps_6_April_2021.dta"
//Now harmonize the variables for appending to the drugs records
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_post_idx_first_hosps_6_April_2021.dta"

//Indicate that state 5 is rehospitalization
gen state = 5

//take only one record per person
sort mbun episode_beg_dt
by mbun: keep if _n==1


//out of 7,566 schizophrenia who were medicated after index,
//1,809 were immediately rehospitalized before first dispensing record
//giving 5,757, no check if these people had at least 6 weeks of antipsychotic tx (not polypharm)
//Another 426 did not have a full 6-week course of medications before rehospitalization
//Another 194 did not have a a full 6-week course of medications but were not rehospitalized

//Another 562 had a gap in drug treatment before 6 weeks completed (file: CIHI_initial_drug_durations_uniq.dta)
*Effective sample: 4,575

//no initial self-harm
//no initial polypharmacy (i.e. SGA monotherapy initially)
//not initially on clz
//not rehospitalized before having 6 weeks of initial SGA treatment


use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_med_gap_calc_19_Mar_21.dta"
keep mbun episode_beg_dt episode_end_dt drug_code drug_desc
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_6_April_2021.dta"

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_6_April_2021.dta"

//Now append the hospitalizations
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_post_idx_first_hosps_6_April_2021.dta"
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"

//Now delete the meds coming after the first rehospitalization

bysort mbun (episode_beg_dt): gen marker = _n if state < .
bysort mbun (episode_beg_dt): egen delete_recs_after = min(marker)

//Some records that do not have rehosp are state 6 (no reshosp)
bysort mbun (episode_beg_dt): gen rec_ctr = _n
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"
bysort mbun (episode_beg_dt): drop if rec_ctr > delete_recs_after & rec_ctr <.

drop if delete_recs_after==1
save, replace

//Merge the day 1 variable to ensure at least 6 weeks initial SGAs
gen days_from_day1 = episode_beg_dt-day1 if state==5

//update the unique cohort list delete the people without 6-wk course of SGA
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_uniq_6_Apr_2021.dta"

//check that the analysis file has correct number of patients
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_uniq_6_Apr_2021.dta"
drop if _m==1
save, replace


//create a subgroup that were not rehospitalized (state 6)

bysort mbun (episode_beg_dt): keep if delete_recs_after==.
bysort mbun (episode_beg_dt): keep if _n==_N
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_patients_not_rehosp_6_April_2021.dta"
replace state = 6

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"
//verify that each one had at least 6-week initial
bysort mbun (episode_beg_dt): egen max_rec = max(rec_ctr)
bysort mbun (episode_beg_dt): gen med_duration = episode_end_dt-episode_beg_dt if max_rec==1


//there are 180 people who did not have a full 6-week regimen but were not hospitalized
//these have to be deleted also
//file: did_not_have_6_weeks_initial_SGA.dta

//Now check those who were not rehospitalized if they had at least 42 days of SGA

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"
bysort mbun (episode_beg_dt): gen last_med_record = 1 if delete==. & _n==_N
gen drug_duration = episode_end_dt - day1 if last_med_record==1

keep if drug_duration < 42
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\for_append_did_not_have_6_weeks_intial_SGA.dta"


/*For 7 April: From the medications only file, calculate the duration of the first medication duration. include only those with 42 days+*/
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_med_gap_calc_19_Mar_21.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_uniq_6_Apr_2021.dta"
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_for_calc_init_med_6wks.dta"
bysort mbun (episode_beg_dt): gen rec_num=_n
bysort mbun (episode_beg_dt): egen max_recs = max(rec_num)
bysort mbun (episode_beg_dt): gen first_med_record = 1 if _n==1
bysort mbun (episode_beg_dt): gen first_med_record_dt = episode_beg_dt if first_med_record == 1
bysort mbun (episode_beg_dt): gen last_med_record = 1 if _n==_N
bysort mbun (episode_beg_dt): gen last_med_record_dt = episode_end_dt if last_med_record == 1

format first_med_record_dt last_med_record_dt %td

bysort mbun (episode_beg_dt): egen first_med_record_dt_k = min(first_med_record_dt)


format first_med_record_dt_k last_med_record_dt_k %td

//add a procedure for minimum gap
//and replace the last med record

bysort mbun (episode_beg_dt) gap: gen first_gap = 1 if gap==1 & _n==1
replace last_med_record_dt = gap_start if first_gap==1

//calculate duration by max_rec
bysort mbun (episode_beg_dt): egen last_med_record_dt_k = min(last_med_record_dt)
format last_med_record_dt_k %td

//now calculate duration of first drug spell
gen init_drug_dur = last_med_record_dt_k- first_med_record_dt_k

//need to aggregate by person
bysort mbun (episode_beg_dt): keep if _n==1

//Now use the longitudinal med file and delete the ones with durations < 42
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_initial_drug_durations_uniq.dta", keepusing(init_drug_dur)
drop if init <42

//Update the uniques file
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_uniq_6_Apr_2021.dta"
merge 1:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_initial_drug_durations_uniq.dta", keepusing(init_drug_dur)
drop if init <42
save, replace

//Now use the longitudinal file and create a list of gaps
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"

//first mark the last rec (earlier of first hosp or med data runs out)
bysort mbun (episode_beg_dt): gen ult_rec = 1 if _n==_N
gen day_ult = episode_end_dt if ult_rec==1
bysort mbun (episode_beg_dt): egen day_ult_k = min(day_ult)
format day_ult_k %td

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_gaps_8_Apr_2021.dta"

replace episode_beg_dt = gap_start if episode_beg_dt==.
replace episode_end_dt = gap_end if episode_end_dt==.

//distribute the day_ult_k variable again
bysort mbun (episode_beg_dt): replace day_ult_k = day_ult_k[_n-1] if day_ult_k==.
replace state=3 if gap_start < .
sort mbun episode_beg_dt

//drop the gaps after the last record of each person
drop if episode_beg_dt > day_ult_k

//9 April 2021
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"
replace drug_code="" if gap_start <.
replace drug_desc="" if gap_end < .

//10 April 2021
//Polypharmacy Procedure
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_med_gap_calc_19_Mar_21.dta"
//limit to patients in cohort
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_uniq_6_Apr_2021.dta"
//delete gaps
drop if gap==1
drop obs-dr_change
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_PP_calc_10_April_2021.dta"

//first merge day_ult_k to the unique_patients
//this is the 4,575 people
//then take the day_ult_k variable
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_PP_calc_10_April_2021.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_uniq_6_Apr_2021.dta", keepusing(day_ult_k)

//now drop the medication records coming after day_ult_k

drop if episode_beg_dt > day_ult_k
save, replace

//Here PP again

bysort mbun (episode_beg_dt): gen poly_beg_dt = episode_beg_dt if polypharm[_n]==1 & polypharm[_n-1]==0
gen poly_end_dt = episode_end_dt if polypharm==1 & overlap==0
format poly_beg_dt %td
format poly_end_dt %td
//these are new polypharmacy spells within the same person that are
//adjacent to a previous spell
bysort mbun (episode_beg_dt): replace poly_beg_dt= episode_beg_dt if overlap[_n-1]==0 & overlap[_n]>0 & polypharm[_n]==1 & poly_beg_dt[_n]==.
bysort mbun (episode_beg_dt): replace poly_beg_dt = poly_beg_dt[_n-1] if poly_beg_dt[_n] == .  & poly_beg_dt[_n-1]<.


//11 April 2021
//Here we use the newspell Stata command for polypharmacy
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_PP_calc_10_April_2021.dta"
//create a spell number per person 
//gap first
bysort mbun (episode_beg_dt): gen spell_gap_pre = _n
order mbun spell_gap_pre
//Need to first detect and fill in gaps
newspell gaps, ncode(GAP) first(individual) last(individual) id(mbun) stype(drug_desc) snumber(spell_gap_pre) ///
begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_gap_post) sort(episode_beg_dt)

replace drug_code="GAP" if drug_desc=="GAP"
*Tabulating the drug_desc, GAPS account for 18% of the entries in drug_desc
/*
//Now consolidate the spells with the same drug_code
bysort mbun (episode_beg_dt): gen new_episode_end_dt = episode_end_dt if drug_code==drug_code[_n-1]
format new_episode_end_dt %td

//create an indicator of last record before gap or new drug_code

bysort mbun (episode_beg_dt): gen last_of_first_cons = 1 if spell_gap_post > 1 & new_episode_end_dt[_n+1]==.
replace last_of_first_cons=0 if drug_code=="GAP"
bysort mbun (episode_beg_dt): gen new_episode_end_dt = episode_end_dt[_n+1] if last_of_first_cons==. & last_of_first_cons[_n+1]==1

//mark the beginning for each person
bysort mbun (episode_beg_dt): gen new_episode_beg_dt = episode_beg_dt if _n==1
format new_episode_beg_dt %td
bysort mbun (episode_beg_dt): replace new_episode_beg_dt = episode_beg_dt if drug_code!="GAP" & drug_code!= drug_code[_n-1] & new_episode_beg_dt==.

//carry over the initial beg date until there is a new episode beg dataset
bysort mbun (episode_beg_dt): replace new_episode_beg_dt = new_episode_beg_dt[_n-1] if new_episode_beg_dt==. & drug_code!="GAP"



//create a gap counter

gen gap = 1 if drug_code=="GAP"
bysort mbun (episode_beg_dt): gen gap_ctr = sum(gap)
bysort mbun (episode_beg_dt): egen max_gap_k = max(gap_ctr)



//Note that the maximum gaps for a person is: 79
bysort mbun (episode_beg_dt): drop if max_gap_k==79 & gap_ctr==79 & gap==.

*/


***********************************

/*Merging records of the same drug after GAPS have been identified/

This procedure is a general procedure that works for both patients with polypharm or no polypharm

*/


***************************************
 use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_PP_calc_10_April_2021.dta"
//use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\temp_pp.dta"
//use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\temp.dta"

gen new_episode_beg_dt = .
gen new_episode_end_dt = .
replace new_episode_beg_dt = episode_beg_dt if drug_code=="GAP"
replace new_episode_end_dt = episode_end_dt if drug_code=="GAP"
replace new_episode_beg_dt = . if drug_code!="GAP"
replace new_episode_end_dt = . if drug_code!="GAP"

//sort by drug code so the adjacent ones can be merged
sort mbun drug_code episode_beg_dt


by mbun: gen merge=1 if drug_code==drug_code[_n-1] & episode_beg_dt-episode_end_dt[_n-1] <= 1

order mbun episode_beg_dt- new_episode_end_dt merge
sort mbun drug_code episode_beg_dt
replace new_episode_beg_dt = episode_beg_dt if merge==.
//replace new_episode_end_dt = episode_end_dt if merge==.

//take the very last record for each drug as the end date
gsort mbun drug_code -episode_end_dt
by mbun drug_code: replace new_episode_end_dt = episode_end_dt if _n==1 | (merge==1 & merge[_n-1]==.)
//this fills in the end dates with the latest in a series
by mbun: replace new_episode_end_dt=new_episode_end_dt[_n-1] if merge==1 & merge[_n-1]!=.

//fill in the begin dates
sort mbun drug_code episode_beg_dt
by mbun drug_code: replace new_episode_beg_dt = new_episode_beg_dt[_n-1] if merge==1 & new_episode_beg_dt==.

//drop the leading records in a group to be merged
bysort mbun drug_code new_episode_beg_dt: drop if new_episode_end_dt==. & merge==. & new_episode_end_dt[_n+1]<.
//assign new end dates if still missing
replace new_episode_end_dt = episode_end_dt if new_episode_end_dt==. & merge==.

duplicates tag mbun drug_code new_episode_beg_dt new_episode_end_dt, gen(dup_rows)
order mbun drug_code drug_desc new_episode_beg_dt new_episode_end_dt merge dup_rows

bysort mbun drug_code new_episode_beg_dt new_episode_end_dt dup_rows: keep if _n==1

sort mbun drug_code new_episode_beg_dt
format new_episode_beg_dt %td
format new_episode_end_dt %td
ren new_episode_beg_dt episode_beg_dt
ren new_episode_end_dt episode_end_dt
drop merge dup_rows spell_gap_post
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_same_drugs_merged_12_April_2021.dta"
**********************************************
drop spell_gap_pre spell_gap_post

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_same_drugs_merged_12_April_2021.dta"
ren new_episode_beg_dt episode_beg_dt
ren new_episode_end_dt episode_end_dt

//need to create a polypharm indicator first at the mbun level
bysort mbun (episode_beg_dt): gen polypharm = 1 if episode_beg_dt - episode_end_dt[_n-1] <0 & drug_code!="GAP"
bysort mbun (episode_beg_dt): replace polypharm = 1 if episode_beg_dt[_n+1]- episode_end_dt <0 & drug_code[_n+1]!="GAP"

save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_pairwise_polypharm.dta"

sort mbun drug_code episode_beg_dt
duplicates tag mbun drug_code, gen(comb_drugs)

bysort mbun drug_code comb_drugs: keep if _n==1

by mbun: gen drug_k = _n
keep mbun drug_code drug_desc drug_k
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_pp_22_Apr_for_reshape.dta"
reshape wide drug_code drug_desc, i(mbun) j(drug_k)
drop drug_code1 drug_code2 drug_code3 drug_code4 drug_code5 drug_code6 drug_code7
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_pairwise_polypharm_reshaped.dta"
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_pairwise_polypharm_reshaped.dta"
duplicates tag drug_desc1 drug_desc2, gen(pairs)


//The following file contains the realizations of PP: pairs, trios, quads, up to septuplets
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_pairwise_polypharm_reshaped_pairs.dta"
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_pairwise_polypharm_reshaped_pairs.dta"

gsort drug_desc1 drug_desc2 -drug_desc3

//We need to recode the drug names
//Create 7 numeric variables
forvalues i=1/7{
    gen drug_`i'=.
}

forvalues i=1/7{
    replace drug_`i' = 1 if drug_desc`i'=="ARIPIPRAZOLE"
	replace drug_`i' = 2 if drug_desc`i'=="ASANEPINE"
	replace drug_`i' = 3 if drug_desc`i'=="CHLORPROMAZINE"
	replace drug_`i' = 4 if drug_desc`i'=="CLOZAPINE"
	replace drug_`i' = 5 if drug_desc`i'=="FLUPENTIXOL"
	replace drug_`i' = 6 if drug_desc`i'=="FLUPHENAZINE"
	replace drug_`i' = 7 if drug_desc`i'=="HALOPERIDOL"
	replace drug_`i' = 8 if drug_desc`i'=="LEVOMEPROMAZINE"
	replace drug_`i' = 9 if drug_desc`i'=="LITHIUM"
	replace drug_`i' = 10 if drug_desc`i'=="LOXAPINE"
	replace drug_`i' = 11 if drug_desc`i'=="LURASIDONE"
	replace drug_`i' = 12 if drug_desc`i'==	"OLANZAPINE"
	replace drug_`i' = 13 if drug_desc`i'=="PALIPERIDONE"
	replace drug_`i' = 14 if drug_desc`i'=="PERPHENAZINE"
	replace drug_`i' = 15 if drug_desc`i'=="PIMOZIDE"
	replace drug_`i' = 16 if drug_desc`i'=="PIPOTIAZINE"
	replace drug_`i' = 17 if drug_desc`i'=="PROCHLORPERAZINE"
	replace drug_`i' = 18 if drug_desc`i'=="QUETIAPINE"
	replace drug_`i' = 19 if drug_desc`i'=="RISPERIDONE"
	replace drug_`i' = 20 if drug_desc`i'==	"Sulpiride"
	replace drug_`i' = 21 if drug_desc`i'=="TRIFLUOPERAZINE"
	replace drug_`i' = 22 if drug_desc`i'=="ZIPRASIDONE"
	replace drug_`i' = 23 if drug_desc`i'=="ZUCLOPENTHIXOL"
}
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_pairwise_polypharm_reshaped_pairs.dta", replace

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_same_drugs_merged_12_April_2021.dta"

gen drug_num=.
order mbun drug_num drug_code drug_desc
replace drug_num = 1 if drug_desc=="ARIPIPRAZOLE"
replace drug_num = 2 if drug_desc=="ASANEPINE"
replace drug_num = 3 if drug_desc=="CHLORPROMAZINE"
replace drug_num = 4 if drug_desc=="CLOZAPINE"
replace drug_num = 5 if drug_desc=="FLUPENTIXOL"
replace drug_num = 6 if drug_desc=="FLUPHENAZINE"
replace drug_num = 7 if drug_desc=="HALOPERIDOL"
replace drug_num = 8 if drug_desc=="LEVOMEPROMAZINE"
replace drug_num = 9 if drug_desc=="LITHIUM"
replace drug_num = 10 if drug_desc=="LOXAPINE"
replace drug_num = 11 if drug_desc=="LURASIDONE"
replace drug_num = 12 if drug_desc=="OLANZAPINE"
replace drug_num = 13 if drug_desc=="PALIPERIDONE"
replace drug_num = 14 if drug_desc=="PERPHENAZINE"
replace drug_num = 15 if drug_desc=="PIMOZIDE"
replace drug_num = 16 if drug_desc=="PIPOTIAZINE"
replace drug_num = 17 if drug_desc=="PROCHLORPERAZINE"
replace drug_num = 18 if drug_desc=="QUETIAPINE"
replace drug_num = 19 if drug_desc=="RISPERIDONE"
replace drug_num = 20 if drug_desc=="Sulpiride"
replace drug_num = 21 if drug_desc=="TRIFLUOPERAZINE"
replace drug_num = 22 if drug_desc=="ZIPRASIDONE"
replace drug_num = 23 if drug_desc=="ZUCLOPENTHIXOL"
replace drug_num=24 if drug_desc=="GAP"


bysort mbun (episode_beg_dt): gen spell_num=_n
order mbun spell_num drug_num
//In the following newspell commands, the new codes are the mbuns in 
//CIHI_pairwise_polypharm_reshaped_pairs.dta

//Added 21 April 2021
//run these also for those with correct histories

newspell combine, combine(12 16) id(mbun) ///
stype(drug_num) snumber(spell_post3) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post4) /// 
sort(episode_beg_dt) ncode(5246) 
drop spell_post3 split
save, replace

newspell combine, combine(19 3) id(mbun) ///
stype(drug_num) snumber(spell_post4) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post5) /// 
sort(episode_beg_dt) ncode(6696) 
drop spell_post4 split
save, replace




newspell combine, combine(1 12) id(mbun) ///
stype(drug_num) snumber(spell_n_2562) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_10463) /// 
sort(episode_beg_dt) ncode(10463) 
drop spell_n_2562 split
save, replace

newspell combine, combine(1 18) id(mbun) ///
stype(drug_num) snumber(spell_n_10463) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_8859) /// 
sort(episode_beg_dt) ncode(8859) 
drop spell_n_10463 split
save, replace

newspell combine, combine(1 19) id(mbun) ///
stype(drug_num) snumber(spell_n_10463) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_24311) /// 
sort(episode_beg_dt) ncode(24311) 
drop spell_n_10463 split
save, replace

newspell combine, combine(13 18) id(mbun) ///
stype(drug_num) snumber(spell_n_10463) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1447) /// 
sort(episode_beg_dt) ncode(1447) 
drop spell_n_10463 split
save, replace

newspell combine, combine(13 19) id(mbun) ///
stype(drug_num) snumber(spell_n_1447) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_27763) /// 
sort(episode_beg_dt) ncode(27763) 
drop spell_n_1447 split
save, replace



newspell combine, combine(13 1) id(mbun) ///
stype(drug_num) snumber(spell_n_27763) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_37649) /// 
sort(episode_beg_dt) ncode(37649) 
drop spell_n_27763 split
save, replace

newspell combine, combine(18 12) id(mbun) ///
stype(drug_num) snumber(spell_n_27763) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_17602) /// 
sort(episode_beg_dt) ncode(17602) 
drop spell_n_27763 split
save, replace

////////////////////////
newspell combine, combine(12 22) id(mbun) ///
stype(drug_num) snumber(spell_post6) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post7) /// 
sort(episode_beg_dt) ncode(2623) 
drop spell_post6 split
save, replace

newspell combine, combine(7 8) id(mbun) ///
stype(drug_num) snumber(spell_post7) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post8) /// 
sort(episode_beg_dt) ncode(2628) 
drop spell_post7 split
save, replace

newspell combine, combine(19 18 18 18 18 ) id(mbun) ///
stype(drug_num) snumber(spell_post8) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post9) /// 
sort(episode_beg_dt) ncode(25622562) 
save, replace


newspell combine, combine(10 7) id(mbun) ///
stype(drug_num) snumber(spell_post9) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post10) /// 
sort(episode_beg_dt) ncode(6102) 
drop spell_post9 split
save, replace

newspell combine, combine(4 1) id(mbun) ///
stype(drug_num) snumber(spell_post10) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post11) /// 
sort(episode_beg_dt) ncode(9064) 
drop spell_post10 split

replace drug_num=8275 if drug_num==9064
save, replace




////////////////////////////////////////
newspell combine, combine(19 12) id(mbun) ///
stype(drug_num) snumber(spell_n_17602) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post1) /// 
sort(episode_beg_dt) ncode(987) 
drop spell_n_17602 split

newspell combine, combine(19 18) id(mbun) ///
stype(drug_num) snumber(spell_n) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_2562) /// 
sort(episode_beg_dt) ncode(2562) 
drop spell_n split



newspell combine, combine(18 5) id(mbun) ///
stype(drug_num) snumber(spell_n_2562) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_18068) /// 
sort(episode_beg_dt) ncode(18068) 
drop spell_n_2562 split


newspell combine, combine(13 12) id(mbun) ///
stype(drug_num) snumber(spell_n_17602) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1298) /// 
sort(episode_beg_dt) ncode(1298) 
drop spell_n_17602 split


newspell combine, combine(12 4) id(mbun) ///
stype(drug_num) snumber(spell_n_2562) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1326) /// 
sort(episode_beg_dt) ncode(1326) 
drop spell_n_2562 split


newspell combine, combine(22 18) id(mbun) ///
stype(drug_num) snumber(spell_n_2562) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_766) /// 
sort(episode_beg_dt) ncode(766) 
drop spell_n_2562 split


newspell combine, combine(21 18) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_13169) /// 
sort(episode_beg_dt) ncode(1803) 
drop spell_n_1591230 split


newspell combine, combine(1 13) id(mbun) ///
stype(drug_num) snumber(spell_num) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_13169) /// 
sort(episode_beg_dt) ncode(13169) 
drop spell_num split

newspell combine, combine(3 5 12) id(mbun) ///
stype(drug_num) snumber(spell_n_13169) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_4064) /// 
sort(episode_beg_dt) ncode(4064) 
drop spell_n_13169 split

newspell combine, combine(4 1) id(mbun) ///
stype(drug_num) snumber(spell_n_13169) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_8275) /// 
sort(episode_beg_dt) ncode(8275) 
drop spell_n_13169 split


newspell combine, combine(4 13) id(mbun) ///
stype(drug_num) snumber(spell_n_8275) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_2112) /// 
sort(episode_beg_dt) ncode(2112) 
drop spell_n_8275 split

newspell combine, combine(4 18) id(mbun) ///
stype(drug_num) snumber(spell_n_2112) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_14583) /// 
sort(episode_beg_dt) ncode(14583) 
drop spell_n_2112 split

newspell combine, combine(4 19) id(mbun) ///
stype(drug_num) snumber(spell_n_14583) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_20390) /// 
sort(episode_beg_dt) ncode(20390) 
drop spell_n_14583 split

newspell combine, combine(5 1) id(mbun) ///
stype(drug_num) snumber(spell_n_20390) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_10577) /// 
sort(episode_beg_dt) ncode(10577) 
drop spell_n_20390 split

//////
newspell combine, combine(5 4) id(mbun) ///
stype(drug_num) snumber(spell_n_10577) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_6322) /// 
sort(episode_beg_dt) ncode(6322) 
drop spell_n_10577 split

newspell combine, combine(5 18) id(mbun) ///
stype(drug_num) snumber(spell_n_6322) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_27062) /// 
sort(episode_beg_dt) ncode(27062) 
drop spell_n_6322 split

newspell combine, combine(5 19) id(mbun) ///
stype(drug_num) snumber(spell_n_27062) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_45762) /// 
sort(episode_beg_dt) ncode(45762) 
drop spell_n_27062 split

newspell combine, combine(6 7) id(mbun) ///
stype(drug_num) snumber(spell_n_45762) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_23196) /// 
sort(episode_beg_dt) ncode(23196) 
drop spell_n_45762 split


//////
newspell combine, combine(6 12) id(mbun) ///
stype(drug_num) snumber(spell_n_23196) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1839) /// 
sort(episode_beg_dt) ncode(1839) 
drop spell_n_23196 split

newspell combine, combine(6 18) id(mbun) ///
stype(drug_num) snumber(spell_n_1839) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_8272) /// 
sort(episode_beg_dt) ncode(8272) 
drop spell_n_1839 split

newspell combine, combine(6 19) id(mbun) ///
stype(drug_num) snumber(spell_n_8272) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_15822) /// 
sort(episode_beg_dt) ncode(15822) 
drop spell_n_8272 split

newspell combine, combine(6 22) id(mbun) ///
stype(drug_num) snumber(spell_n_15822) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3605) /// 
sort(episode_beg_dt) ncode(3605) 
drop spell_n_15822 split
//////
newspell combine, combine(7 12) id(mbun) ///
stype(drug_num) snumber(spell_n_3605) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3532) /// 
sort(episode_beg_dt) ncode(3532) 
drop spell_n_3605 split

 newspell combine, combine(7 19) id(mbun) ///
stype(drug_num) snumber(spell_n_3532) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_7250) /// 
sort(episode_beg_dt) ncode(7250) 
drop spell_n_3532 split

newspell combine, combine(7 22) id(mbun) ///
stype(drug_num) snumber(spell_n_7250) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_16497) /// 
sort(episode_beg_dt) ncode(16497) 
drop spell_n_7250 split

 newspell combine, combine(8 1) id(mbun) ///
stype(drug_num) snumber(spell_n_16497) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_12702) /// 
sort(episode_beg_dt) ncode(12702) 
drop spell_n_16497 split

 newspell combine, combine(8 13) id(mbun) ///
stype(drug_num) snumber(spell_n_12702) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_27125) /// 
sort(episode_beg_dt) ncode(27125) 
drop spell_n_12702 split

 newspell combine, combine(8 18) id(mbun) ///
stype(drug_num) snumber(spell_n_27125) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_46014) /// 
sort(episode_beg_dt) ncode(46014) 
drop spell_n_27125 split

 newspell combine, combine(8 19) id(mbun) ///
stype(drug_num) snumber(spell_n_46014) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_683) /// 
sort(episode_beg_dt) ncode(683) 
drop spell_n_46014 split

 newspell combine, combine(9 13) id(mbun) ///
stype(drug_num) snumber(spell_n_683) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_19629) /// 
sort(episode_beg_dt) ncode(19629) 
drop spell_n_683 split

 
newspell combine, combine(9 19) id(mbun) ///
stype(drug_num) snumber(spell_n_19629) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_6498) /// 
sort(episode_beg_dt) ncode(6498) 
drop spell_n_19629 split

 newspell combine, combine(10 18) id(mbun) ///
stype(drug_num) snumber(spell_n_6498) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_12018) /// 
sort(episode_beg_dt) ncode(12018) 
drop spell_n_6498 split

newspell combine, combine(10 19) id(mbun) ///
stype(drug_num) snumber(spell_n_12018) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_51919) /// 
sort(episode_beg_dt) ncode(51919) 
drop spell_n_12018 split

 newspell combine, combine(12 1) id(mbun) ///
stype(drug_num) snumber(spell_n_51919) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_55675) /// 
sort(episode_beg_dt) ncode(55675) 
drop spell_n_51919 split

 newspell combine, combine(12 9) id(mbun) ///
stype(drug_num) snumber(spell_n_55675) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_56440) /// 
sort(episode_beg_dt) ncode(56440) 
drop spell_n_55675 split

 newspell combine, combine(12 13) id(mbun) ///
stype(drug_num) snumber(spell_n_56440) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_56831) /// 
sort(episode_beg_dt) ncode(56831) 
drop spell_n_56440 split

 newspell combine, combine(12 18) id(mbun) ///
stype(drug_num) snumber(spell_n_56831) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_35663) /// 
sort(episode_beg_dt) ncode(35663) 
drop spell_n_56831 split

 newspell combine, combine(12 19) id(mbun) ///
stype(drug_num) snumber(spell_n_35663) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3038) /// 
sort(episode_beg_dt) ncode(3038) 
drop spell_n_35663 split

//doesn't overlap
newspell combine, combine(14 23) id(mbun) ///
stype(drug_num) snumber(spell_n_3038) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_39709) /// 
sort(episode_beg_dt) ncode(39709) 
drop spell_n_3038 split

newspell combine, combine(15 12) id(mbun) ///
stype(drug_num) snumber(spell_n_3038) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_10340) /// 
sort(episode_beg_dt) ncode(10340) 
drop spell_n_3038 split

//doesn't overlap
newspell combine, combine(15 13) id(mbun) ///
stype(drug_num) snumber(spell_n_10340) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_36945) /// 
sort(episode_beg_dt) ncode(36945) 
drop spell_n_10340 split

 newspell combine, combine(17 18) id(mbun) ///
stype(drug_num) snumber(spell_n_10340) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_16591) /// 
sort(episode_beg_dt) ncode(16591) 
drop spell_n_10340 split

 newspell combine, combine(18 1) id(mbun) ///
stype(drug_num) snumber(spell_n_16591) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_25297) /// 
sort(episode_beg_dt) ncode(25297) 
drop spell_n_16591 split

 newspell combine, combine(18 9) id(mbun) ///
stype(drug_num) snumber(spell_n_25297) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_20766) /// 
sort(episode_beg_dt) ncode(20766) 
drop spell_n_25297 split

 newspell combine, combine(18 13) id(mbun) ///
stype(drug_num) snumber(spell_n_20766) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_24179) /// 
sort(episode_beg_dt) ncode(24179) 
drop spell_n_20766 split

 newspell combine, combine(18 19) id(mbun) ///
stype(drug_num) snumber(spell_n_24179) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_48657) /// 
sort(episode_beg_dt) ncode(48657) 
drop spell_n_24179 split

 
newspell combine, combine(19 1) id(mbun) ///
stype(drug_num) snumber(spell_n_48657) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_53025) /// 
sort(episode_beg_dt) ncode(53025) 
drop spell_n_48657 split

 newspell combine, combine(19 13) id(mbun) ///
stype(drug_num) snumber(spell_n_53025) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_27704) /// 
sort(episode_beg_dt) ncode(27704) 
drop spell_n_53025 split

newspell combine, combine(21 1) id(mbun) ///
stype(drug_num) snumber(spell_n_27704) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_38602) /// 
sort(episode_beg_dt) ncode(38602) 
drop spell_n_27704 split

 newspell combine, combine(21 19) id(mbun) ///
stype(drug_num) snumber(spell_n_38602) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_24908) /// 
sort(episode_beg_dt) ncode(24908) 
drop spell_n_38602 split

newspell combine, combine(22 1) id(mbun) ///
stype(drug_num) snumber(spell_n_24908) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_38013) /// 
sort(episode_beg_dt) ncode(38013) 
drop spell_n_24908 split


newspell combine, combine(22 18) id(mbun) ///
stype(drug_num) snumber(spell_n_35045) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_13951) /// 
sort(episode_beg_dt) ncode(13951) 
drop spell_n_35045 split

 newspell combine, combine(22 23) id(mbun) ///
stype(drug_num) snumber(spell_n_13951) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_47162) /// 
sort(episode_beg_dt) ncode(47162) 
drop spell_n_13951 split

 
newspell combine, combine(23 12) id(mbun) ///
stype(drug_num) snumber(spell_n_47162) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_51110) /// 
sort(episode_beg_dt) ncode(51110) 
drop spell_n_47162 split



////////
newspell combine, combine(23 12) id(mbun) ///
stype(drug_num) snumber(spell_n_17602) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_683683) /// 
sort(episode_beg_dt) ncode(683683) 
drop spell_n_17602 split
replace drug_num=51110 if drug_num==683683
////////////

 newspell combine, combine(23 13) id(mbun) ///
stype(drug_num) snumber(spell_n_17602) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_28189) /// 
sort(episode_beg_dt) ncode(28189) 
drop spell_n_17602 split

newspell combine, combine(23 18) id(mbun) ///
stype(drug_num) snumber(spell_n_28189) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_10022) /// 
sort(episode_beg_dt) ncode(10022) 
drop spell_n_28189 split

 newspell combine, combine(23 19) id(mbun) ///
stype(drug_num) snumber(spell_n_10022) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_700) /// 
sort(episode_beg_dt) ncode(700) 
drop spell_n_10022 split


//////////////
newspell combine, combine(23 19) id(mbun) ///
stype(drug_num) snumber(spell) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_7007) /// 
sort(episode_beg_dt) ncode(7007)
save, replace
drop spell_n_700700 split


replace drug_num=700 if drug_num==7007
save, replace
////////////

newspell combine, combine(23 1 1 1 1) id(mbun) ///
stype(drug_num) snumber(spell_post) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post2) /// 
sort(episode_beg_dt) ncode(231111)
save, replace

newspell combine, combine(10 7 18) id(mbun) ///
stype(drug_num) snumber(spell_post2) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post3) /// 
sort(episode_beg_dt) ncode(10718)
save, replace










//Next, work on the 2-element subsets of the triplets
//14 April 2021

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_same_drugs_merged_12_April_2021.dta"


newspell combine, combine(3 5) id(mbun) ///
stype(drug_num) snumber(spell_n_700) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_406410) /// 
sort(episode_beg_dt) ncode(406410) 
drop spell_n_700 split

newspell combine, combine(3 12) id(mbun) ///
stype(drug_num) snumber(spell_n_406410) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_406420) /// 
sort(episode_beg_dt) ncode(406420) 
drop spell_n_406410 split

//this is okay
newspell combine, combine(4 1) id(mbun) ///
stype(drug_num) snumber(spell_n_406420) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_4166930) /// 
sort(episode_beg_dt) ncode(4166930) 
drop spell_n_406420 split


//already done
newspell combine, combine(4 19) id(mbun) ///
stype(drug_num) snumber(spell_n_2960230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_2960230) /// 
sort(episode_beg_dt) ncode(2960230) 
drop spell_n_2960230 split

//this is okay
newspell combine, combine(5 10) id(mbun) ///
stype(drug_num) snumber(spell_n_2960230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3308410) /// 
sort(episode_beg_dt) ncode(3308410) 
drop spell_n_2960230 split

//this is okay
newspell combine, combine(5 12) id(mbun) ///
stype(drug_num) snumber(spell_n_3308410) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_406430) /// 
sort(episode_beg_dt) ncode(406430) 
drop spell_n_3308410 split

newspell combine, combine(5 18) id(mbun) ///
stype(drug_num) snumber(spell_n_406430) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_375320) /// 
sort(episode_beg_dt) ncode(375320) 
drop spell_n_406430 split

newspell combine, combine(6 5) id(mbun) ///
stype(drug_num) snumber(spell_n_375320) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_615710) /// 
sort(episode_beg_dt) ncode(615710) 
drop spell_n_375320 split

newspell combine, combine(6 12) id(mbun) ///
stype(drug_num) snumber(spell_n_615710) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_524620) /// 
sort(episode_beg_dt) ncode(524620) 
drop spell_n_615710 split
//the next doesn't overlap
newspell combine, combine(6 16) id(mbun) ///
stype(drug_num) snumber(spell_n_524620) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_524610) /// 
sort(episode_beg_dt) ncode(524610) 
drop spell_n_524620 split

newspell combine, combine(6 18) id(mbun) ///
stype(drug_num) snumber(spell_n_524620) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_615720) /// 
sort(episode_beg_dt) ncode(615720) 
drop spell_n_524620 split

newspell combine, combine(7 1) id(mbun) ///
stype(drug_num) snumber(spell_n_615720) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_4166920) /// 
sort(episode_beg_dt) ncode(4166920) 
drop spell_n_615720 split

newspell combine, combine(7 4) id(mbun) ///
stype(drug_num) snumber(spell_n_4166920) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_4166910) /// 
sort(episode_beg_dt) ncode(4166910) 
drop spell_n_4166920 split

newspell combine, combine(7 18) id(mbun) ///
stype(drug_num) snumber(spell_n_4166910) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_2657710) /// 
sort(episode_beg_dt) ncode(2657710) 
drop spell_n_4166910 split


newspell combine, combine(7 23) id(mbun) ///
stype(drug_num) snumber(spell_n_2657710) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_5387810) /// 
sort(episode_beg_dt) ncode(5387810) 
drop spell_n_2657710 split

newspell combine, combine(8 4) id(mbun) ///
stype(drug_num) snumber(spell_n_5387810) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_2960210) /// 
sort(episode_beg_dt) ncode(2960210) 
drop spell_n_5387810 split

//
newspell combine, combine(8 12) id(mbun) ///
stype(drug_num) snumber(spell_n_2960210) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3067210) /// 
sort(episode_beg_dt) ncode(3067210) 
drop spell_n_2960210 split

//the next doesn't overlap
 newspell combine, combine(8 21) id(mbun) ///
stype(drug_num) snumber(spell_n_3067210) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1814110) /// 
sort(episode_beg_dt) ncode(1814110) 
drop spell_n_3067210 split

newspell combine, combine(8 23) id(mbun) ///
stype(drug_num) snumber(spell_n_3067210) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_524110) /// 
sort(episode_beg_dt) ncode(524110) 
drop spell_n_3067210 split

///
newspell combine, combine(10 4) id(mbun) ///
stype(drug_num) snumber(spell_n_524110) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_4086810) /// 
sort(episode_beg_dt) ncode(4086810) 
drop spell_n_524110 split

newspell combine, combine(10 9) id(mbun) ///
stype(drug_num) snumber(spell_n_4086810) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_4701010) /// 
sort(episode_beg_dt) ncode(4701010) 
drop spell_n_4086810 split

 newspell combine, combine(10 12) id(mbun) ///
stype(drug_num) snumber(spell_n_4701010) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3308430) /// 
sort(episode_beg_dt) ncode(3308430) 
drop spell_n_4701010 split

newspell combine, combine(11 1) id(mbun) ///
stype(drug_num) snumber(spell_n_3308430) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1468420) /// 
sort(episode_beg_dt) ncode(1468420) 
drop spell_n_3308430 split

newspell combine, combine(11 12) id(mbun) ///
stype(drug_num) snumber(spell_n_1468420) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_74410) /// 
sort(episode_beg_dt) ncode(74410) 
drop spell_n_1468420 split

newspell combine, combine(11 13) id(mbun) ///
stype(drug_num) snumber(spell_n_74410) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_74420) /// 
sort(episode_beg_dt) ncode(74420) 
drop spell_n_74410 split

newspell combine, combine(11 19) id(mbun) ///
stype(drug_num) snumber(spell_n_74420) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1468410) /// 
sort(episode_beg_dt) ncode(1468410) 
drop spell_n_74420 split
save, replace
/////
newspell combine, combine(16 1) id(mbun) ///
stype(drug_num) snumber(spell_n_1468410) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3472520) /// 
sort(episode_beg_dt) ncode(3472520) 
drop spell_n_1468410 split

 newspell combine, combine(16 12) id(mbun) ///
stype(drug_num) snumber(spell_n_3472520) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_524630) /// 
sort(episode_beg_dt) ncode(524630) 
drop spell_n_3472520 split

newspell combine, combine(16 18) id(mbun) ///
stype(drug_num) snumber(spell_n_524630) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3472510) /// 
sort(episode_beg_dt) ncode(3472510) 
drop spell_n_524630 split
save, replace
/////
//the next one doesnt overlap
newspell combine, combine(21 7) id(mbun) ///
stype(drug_num) snumber(spell_n_3472510) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_5554110) /// 
sort(episode_beg_dt) ncode(5554110) 
drop spell_n_3472510 split

newspell combine, combine(21 12) id(mbun) ///
stype(drug_num) snumber(spell_n_3472510) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_5554120) /// 
sort(episode_beg_dt) ncode(5554120) 
drop spell_n_3472510 split

newspell combine, combine(21 22) id(mbun) ///
stype(drug_num) snumber(spell_n_5554120) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1591210) /// 
sort(episode_beg_dt) ncode(1591210) 
drop spell_n_5554120 split
save, replace
///

newspell combine, combine(22 10) id(mbun) ///
stype(drug_num) snumber(spell_n_1591210) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_5717110) /// 
sort(episode_beg_dt) ncode(5717110) 
drop spell_n_1591210 split

newspell combine, combine(22 12) id(mbun) ///
stype(drug_num) snumber(spell_n_5717110) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1591230) /// 
sort(episode_beg_dt) ncode(1591230) 
drop spell_n_5717110 split
save, replace

//////Tomorrow, do the triplets
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_same_drugs_merged_12_April_2021.dta", clear
//the next one doesnt overlap
newspell combine, combine(7 4 1) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_41669) /// 
sort(episode_beg_dt) ncode(41669) 
drop spell_n_1591230 split
//the next one doesnt overlap
 newspell combine, combine(11 19 1) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_14684) /// 
sort(episode_beg_dt) ncode(14684) 
drop spell_n_1591230 split
//the next one doesnt overlap
 newspell combine, combine(16 18 1) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_34725) /// 
sort(episode_beg_dt) ncode(34725) 
drop spell_n_1591230 split

//done previously
 newspell combine, combine(3 5 12) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_4064) /// 
sort(episode_beg_dt) ncode(4064) 
drop spell_n_1591230 split
//the next one doesnt overlap
 newspell combine, combine(5 10 12) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_33084) /// 
sort(episode_beg_dt) ncode(33084) 
drop spell_n_1591230 split
//the next one doesnt overlap
 newspell combine, combine(6 16 12) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_5246) /// 
sort(episode_beg_dt) ncode(5246) 
drop spell_n_1591230 split
//the next one doesnt overlap
 newspell combine, combine(21 7 12) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_55541) /// 
sort(episode_beg_dt) ncode(55541) 
drop spell_n_1591230 split
//done previously
 newspell combine, combine(21 22 12) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_15912) /// 
sort(episode_beg_dt) ncode(15912) 
drop spell_n_1591230 split

////
//the next one doesnt overlap
newspell combine, combine(11 12 13) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_744) /// 
sort(episode_beg_dt) ncode(744) 
drop spell_n_1591230 split

//the next one doesnt overlap
newspell combine, combine(5 12 18) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3753) /// 
sort(episode_beg_dt) ncode(3753) 
drop spell_n_1591230 split

//the next one doesnt overlap
newspell combine, combine(6 5 18) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_6157) /// 
sort(episode_beg_dt) ncode(6157) 
drop spell_n_1591230 split

//the next one doesnt overlap
newspell combine, combine(8 12 18) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_30672) /// 
sort(episode_beg_dt) ncode(30672) 
drop spell_n_1591230 split
//the next one doesnt overlap
newspell combine, combine(22 10 18) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_57171) /// 
sort(episode_beg_dt) ncode(57171) 
drop spell_n_1591230 split
//the next one doesnt overlap
newspell combine, combine(22 12 18) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_37723) /// 
sort(episode_beg_dt) ncode(37723) 
drop spell_n_1591230 split
//the next one doesnt overlap
newspell combine, combine(7 18 19) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_26577) /// 
sort(episode_beg_dt) ncode(26577) 
drop spell_n_1591230 split
//the next one doesnt overlap
 newspell combine, combine(7 23 19) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_53878) /// 
sort(episode_beg_dt) ncode(53878) 
drop spell_n_1591230 split 
//the next one doesnt overlap
newspell combine, combine(8 4 19) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_29602) /// 
sort(episode_beg_dt) ncode(29602) 
drop spell_n_1591230 split

newspell combine, combine(8 21 19) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_18141) /// 
sort(episode_beg_dt) ncode(18141) 
drop spell_n_1591230 split

newspell combine, combine(8 23 19) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_5241) /// 
sort(episode_beg_dt) ncode(5241) 
drop spell_n_1591230 split

newspell combine, combine(10 4 19) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_40868) /// 
sort(episode_beg_dt) ncode(40868) 
drop spell_n_1591230 split

newspell combine, combine(10 9 19) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_47010) /// 
sort(episode_beg_dt) ncode(47010) 
drop spell_n_1591230 split

newspell combine, combine(21 12 19) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_10963) /// 
sort(episode_beg_dt) ncode(10963) 
drop spell_n_1591230 split

//Now create state numbers
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_same_drugs_merged_12_April_2021.dta", clear
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_15_April_2021.dta"
ren spell_n_1591230 spell_num
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_15_April_2021.dta"
replace drug_code = "POLY" if drug_num > 24 & drug_num < .
replace drug_desc = "POLYPHARMACY" if drug_num > 24 & drug_num < .


//Prepare the unhospitalized patients for appending to a combined hosp and drugs file***
//These peopl will have "CENSOR" and terminal state = 6
//At the end of the replace procedure, the saved file now has final states for the
//censored people
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_not_rehosp_15_April_2021_uniq.dta"

replace drug_code = "CENSOR"
replace drug_desc = "CENSORED"
replace episode_beg_dt = episode_end_dt

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_15_April_2021.dta"
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_not_rehosp_15_April_2021_uniq.dta"

bysort mbun (episode_beg_dt episode_end_dt): replace spell_num = spell_num[_n-1]+1 if spell_num==.

//Now add the final states for the hospitalized people
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_meds_and_rehosp_6_April_2021.dta"
sort mbun episode_beg_dt episode_end_dt
keep if drug_code==.

//First retrieve the hospitalization records of the patients in the cohort
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\hospitalizations_file_15_April_take_the first_rehosp_with_mental_health_dx.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_uniq_6_Apr_2021.dta"
drop if _m==1

//drop index hosps
drop if index_hosp==1

//keep the mental-health related dx's only
keep if any_mental_dx==1

//keep the first mental health dx only and the date it occurred
bysort mbun (episode_beg_dt episode_end_dt): keep if _n==1

//drop unnecessary vars
drop clz index_hosp day0 day1

//check duplicates

duplicates tag mbun, gen(person_clone)
//there are none
drop person_clone

//There are now two files for ascerating censorship or rehospitalization due to any 
//mental health condition
//(1)CIHI_SGA_SCZ_merged_pp_16_April_2021_censored_only.dta
//(2)CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only.dta
//The above two are partitions of CIHI_SGA_SCZ_merged_pp_16_April_2021_hosp_and_censored.dta
//For the rehospitalized people, we need to match the day_ult_k in CIHI_SGA_SCZ_merged_pp_15_April_2021.dta
//to CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only.dta. From there, we may need to add (subtract) medications
//using the final day as point of reference.

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_15_April_2021.dta"
ren day_ult_k day_ult_k_verify
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_16_April_2021_for_verification.dta"
ren episode_beg_dt episode_beg_dt_verify
ren episode_end_dt episode_end_dt_verify

//the result of the next line
//yields rehosp dates for the rehospitalized (episode_beg_dt)
//if the episode_beg_dt is null, this means that the person was not rehospitalization for a mental health dx
merge 1:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_hosp_and_censored.dta", keepusing(any_mental_dx episode_beg_dt episode_end_dt) generate(_merge_w_cens_hosp)
drop if 

save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_16_April_2021_for_verification.dta", replace

keep if episode_beg_dt==.
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_16_April_2021_set_these_recs_to_state_6.dta"

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_15_April_2021_for_correcting.dta"
replace state=6 if _m==3
replace drug_code = "CENSOR" if state==6
replace drug_desc = "CENSORED" if state==6

//the following file is a unique patients file
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only.dta"
gen wrong_day_ult_k = 1 if day_ult_k!= episode_beg_dt
gen day_ult_k_correct = episode_beg_dt if wrong==1

//there are 1,457 people with wrong last day
//we need to retrieve the last drug_code taken by these people from the CIHI_SGA_SCZ_merged_pp_15_April_2021.dta
keep if wrong_day_ult_k==1
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_16_April_2021_rehosp_only_for_retrieve_med.dta"

//take the very last medication in the analysis file
 merge 1:m mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_15_April_2021.dta"
 
 
 **********************Use the file with meds and take the last medication
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_15_April_2021_for_correcting.dta"
drop if state==5
drop state
bysort mbun (episode_beg_dt): gen last_med=1 if _n==_N
keep if last_med==1

***************************
//Now go to the rehospitalized file
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only.dta"
merge 1:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_last_meds.dta", keepusing(drug_num-episode_end_dt_verify day_ult_k_chk_this last_med)
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only_last_meds.dta"

ren episode_beg_dt_verify episode_beg_dt_last_drug
ren episode_end_dt_verify episode_end_dt_last_drug

replace day_ult_k_correct = episode_beg_dt if day_ult_k_correct==.
save, replace
//now we have to take the longitudinal record of drugs for those patients in CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only_last_meds.dta
//taking only the drugs taken after the episode_end_dt_last_drug and before drug_ult_k_correct

ren drug_num last_drug_num
ren drug_code last_drug_c
ren drug_desc last_drug_de
drop last_med
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only_last_meds.dta"

ren episode_beg_dt episode_beg_dt_last_hosp
ren episode_end_dt episode_end_dt_last_hosp

//don't use the file in the next line because the meds have been truncated already to wrong last hospitalization
merge 1:m mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_All_Drugs_Treated_Patients.dta", keepusing(drug_code drug_desc episode_beg_dt episode_end_dt)

//drop the drugs coming after day_ult_k_correct
drop if episode_beg_dt > day_ult_k_correct
drop if episode_beg_dt < episode_beg_dt_last_drug

ren last_drug_de last_drug_de_wrong
ren last_drug_c last_drug_c_wrong

gen drug_differs = 1 if last_drug_de_wrong!= drug_desc & last_drug_de_wrong!="POLYPHARMACY"
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only_additional_meds.dta"

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only_additional_meds.dta"

//Need to create a subset of people with additional drugs
//This can be done by creating a max(drug_differs)
//Then add the drugs from the first discrepancy to the final day(day_ult_k)

bysort mbun (episode_beg_dt): egen has_addl_meds = max(drug_differs)
keep if has_addl_meds==1

//Now we have to repeat the drug gaps, merging, combining for PP.

bysort mbun (episode_beg_dt): gen spell_gap_pre = _n
order mbun spell_gap_pre

newspell gaps, ncode(GAP) first(individual) last(individual) id(mbun) stype(drug_desc) snumber(spell_gap_pre) ///
begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_gap_post) sort(episode_beg_dt)

drop spell_gap_pre
replace drug_code = "GAP" if drug_desc=="GAP"

*--------------------------------------------------------------------------
//Reuse the merging of same drugs procedure

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_no_sh_PP_calc_10_April_2021.dta"
//use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\temp_pp.dta"
//use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\temp.dta"
gen new_episode_beg_dt = episode_beg_dt if drug_code=="GAP"
gen new_episode_end_dt = episode_end_dt if drug_code=="GAP"
replace new_episode_beg_dt = . if drug_code!="GAP"
replace new_episode_end_dt = . if drug_code!="GAP"

//sort by drug code so the adjacent ones can be merged
sort mbun drug_code episode_beg_dt


by mbun: gen merge=1 if drug_code==drug_code[_n-1] & episode_beg_dt-episode_end_dt[_n-1] <= 1

order mbun episode_beg_dt- new_episode_end_dt merge
sort mbun drug_code episode_beg_dt
replace new_episode_beg_dt = episode_beg_dt if merge==.
//replace new_episode_end_dt = episode_end_dt if merge==.

//take the very last record for each drug as the end date
gsort mbun drug_code -episode_end_dt
by mbun drug_code: replace new_episode_end_dt = episode_end_dt if _n==1 | (merge==1 & merge[_n-1]==.)
//this fills in the end dates with the latest in a series
by mbun: replace new_episode_end_dt=new_episode_end_dt[_n-1] if merge==1 & merge[_n-1]!=.

//fill in the begin dates
sort mbun drug_code episode_beg_dt
by mbun drug_code: replace new_episode_beg_dt = new_episode_beg_dt[_n-1] if merge==1 & new_episode_beg_dt==.

//drop the leading records in a group to be merged
bysort mbun drug_code new_episode_beg_dt: drop if new_episode_end_dt==. & merge==. & new_episode_end_dt[_n+1]<.
//assign new end dates if still missing
replace new_episode_end_dt = episode_end_dt if new_episode_end_dt==. & merge==.

duplicates tag mbun drug_code new_episode_beg_dt new_episode_end_dt, gen(dup_rows)
order mbun drug_code drug_desc new_episode_beg_dt new_episode_end_dt merge dup_rows

bysort mbun drug_code new_episode_beg_dt new_episode_end_dt dup_rows: keep if _n==1

sort mbun drug_code new_episode_beg_dt
drop episode_beg_dt episode_end_dt merge has_addl_meds drug_differs
ren new_episode_beg_dt episode_beg_dt
ren new_episode_end_dt episode_end_dt

///Now we have to repeat the PP procedure
gen drug_num=.
order mbun drug_num drug_code drug_desc
replace drug_num = 1 if drug_desc=="ARIPIPRAZOLE"
replace drug_num = 2 if drug_desc=="ASANEPINE"
replace drug_num = 3 if drug_desc=="CHLORPROMAZINE"
replace drug_num = 4 if drug_desc=="CLOZAPINE"
replace drug_num = 5 if drug_desc=="FLUPENTIXOL"
replace drug_num = 6 if drug_desc=="FLUPHENAZINE"
replace drug_num = 7 if drug_desc=="HALOPERIDOL"
replace drug_num = 8 if drug_desc=="LEVOMEPROMAZINE"
replace drug_num = 9 if drug_desc=="LITHIUM"
replace drug_num = 10 if drug_desc=="LOXAPINE"
replace drug_num = 11 if drug_desc=="LURASIDONE"
replace drug_num = 12 if drug_desc=="OLANZAPINE"
replace drug_num = 13 if drug_desc=="PALIPERIDONE"
replace drug_num = 14 if drug_desc=="PERPHENAZINE"
replace drug_num = 15 if drug_desc=="PIMOZIDE"
replace drug_num = 16 if drug_desc=="PIPOTIAZINE"
replace drug_num = 17 if drug_desc=="PROCHLORPERAZINE"
replace drug_num = 18 if drug_desc=="QUETIAPINE"
replace drug_num = 19 if drug_desc=="RISPERIDONE"
replace drug_num = 20 if drug_desc=="Sulpiride"
replace drug_num = 21 if drug_desc=="TRIFLUOPERAZINE"
replace drug_num = 22 if drug_desc=="ZIPRASIDONE"
replace drug_num = 23 if drug_desc=="ZUCLOPENTHIXOL"
replace drug_num=24 if drug_desc=="GAP"

replace drug_num=20 if drug_desc== "Amisulpride"
replace drug_num=16 if drug_desc=="TIOTIXENE" 



newspell combine, combine(1 13) id(mbun) ///
stype(drug_num) snumber(spell_num) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_13169) /// 
sort(episode_beg_dt) ncode(13169) 
drop spell_num split


//3 does not exist
newspell combine, combine(3 5 12) id(mbun) ///
stype(drug_num) snumber(spell_n_13169) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_4064) /// 
sort(episode_beg_dt) ncode(4064) 
drop spell_n_13169 split

//ok
newspell combine, combine(4 1) id(mbun) ///
stype(drug_num) snumber(spell_n_13169) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_8275) /// 
sort(episode_beg_dt) ncode(8275) 
drop spell_n_13169 split

//done
newspell combine, combine(4 13) id(mbun) ///
stype(drug_num) snumber(spell_n_1591230) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_2112) /// 
sort(episode_beg_dt) ncode(2112) 
drop spell_n_8275 split

//ok
newspell combine, combine(4 18) id(mbun) ///
stype(drug_num) snumber(spell_n_8275) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_14583) /// 
sort(episode_beg_dt) ncode(14583) 
drop spell_n_8275 split

//doesn't overlap
newspell combine, combine(4 19) id(mbun) ///
stype(drug_num) snumber(spell_n_14583) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_20390) /// 
sort(episode_beg_dt) ncode(20390) 
drop spell_n_14583 split
//doesn't overlap
newspell combine, combine(5 1) id(mbun) ///
stype(drug_num) snumber(spell_n_14583) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_10577) /// 
sort(episode_beg_dt) ncode(10577) 
drop spell_n_14583 split

//////
//doesn't overlap
newspell combine, combine(5 4) id(mbun) ///
stype(drug_num) snumber(spell_n_14583) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_6322) /// 
sort(episode_beg_dt) ncode(6322) 
drop spell_n_14583 split

//ok
newspell combine, combine(5 18) id(mbun) ///
stype(drug_num) snumber(spell_n_14583) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_27062) /// 
sort(episode_beg_dt) ncode(27062) 
drop spell_n_14583 split

//ok
newspell combine, combine(5 19) id(mbun) ///
stype(drug_num) snumber(spell_n_27062) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_45762) /// 
sort(episode_beg_dt) ncode(45762) 
drop spell_n_27062 split
//doesn't overlap
newspell combine, combine(6 7) id(mbun) ///
stype(drug_num) snumber(spell_n_45762) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_23196) /// 
sort(episode_beg_dt) ncode(23196) 
drop spell_n_45762 split

//////
//doesn't overlap
newspell combine, combine(6 12) id(mbun) ///
stype(drug_num) snumber(spell_n_45762) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_1839) /// 
sort(episode_beg_dt) ncode(1839) 
drop spell_n_45762 split

//ok
newspell combine, combine(6 18) id(mbun) ///
stype(drug_num) snumber(spell_n_45762) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_8272) /// 
sort(episode_beg_dt) ncode(8272) 
drop spell_n_45762 split
//doesn't overlap
newspell combine, combine(6 19) id(mbun) ///
stype(drug_num) snumber(spell_n_8272) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_15822) /// 
sort(episode_beg_dt) ncode(15822) 
drop spell_n_8272 split
//ok
newspell combine, combine(6 22) id(mbun) ///
stype(drug_num) snumber(spell_n_8272) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3605) /// 
sort(episode_beg_dt) ncode(3605) 
drop spell_n_8272 split
//////
//doesn't overlap
newspell combine, combine(7 12) id(mbun) ///
stype(drug_num) snumber(spell_n_3605) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3532) /// 
sort(episode_beg_dt) ncode(3532) 
drop spell_n_3605 split
//doesn't overlap
newspell combine, combine(7 19) id(mbun) ///
stype(drug_num) snumber(spell_n_3605) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_7250) /// 
sort(episode_beg_dt) ncode(7250) 
drop spell_n_3532 split
//doesn't overlap
newspell combine, combine(7 22) id(mbun) ///
stype(drug_num) snumber(spell_n_3605) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_16497) /// 
sort(episode_beg_dt) ncode(16497) 
drop spell_n_3605 split
//doesn't overlap
 newspell combine, combine(8 1) id(mbun) ///
stype(drug_num) snumber(spell_n_3605) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_12702) /// 
sort(episode_beg_dt) ncode(12702) 
drop spell_n_3605 split

 newspell combine, combine(8 13) id(mbun) ///
stype(drug_num) snumber(spell_n_3605) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_27125) /// 
sort(episode_beg_dt) ncode(27125) 
drop spell_n_3605 split

 newspell combine, combine(8 18) id(mbun) ///
stype(drug_num) snumber(spell_n_27125) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_46014) /// 
sort(episode_beg_dt) ncode(46014) 
drop spell_n_27125 split

 newspell combine, combine(8 19) id(mbun) ///
stype(drug_num) snumber(spell_n_46014) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_683) /// 
sort(episode_beg_dt) ncode(683) 
drop spell_n_46014 split

 newspell combine, combine(9 13) id(mbun) ///
stype(drug_num) snumber(spell_n_683) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_19629) /// 
sort(episode_beg_dt) ncode(19629) 
drop spell_n_683 split

 
newspell combine, combine(9 19) id(mbun) ///
stype(drug_num) snumber(spell_n_19629) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_6498) /// 
sort(episode_beg_dt) ncode(6498) 
drop spell_n_19629 split

 newspell combine, combine(10 18) id(mbun) ///
stype(drug_num) snumber(spell_n_6498) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_12018) /// 
sort(episode_beg_dt) ncode(12018) 
drop spell_n_6498 split

newspell combine, combine(10 19) id(mbun) ///
stype(drug_num) snumber(spell_n_12018) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_51919) /// 
sort(episode_beg_dt) ncode(51919) 
drop spell_n_12018 split

 newspell combine, combine(12 1) id(mbun) ///
stype(drug_num) snumber(spell_n_51919) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_55675) /// 
sort(episode_beg_dt) ncode(55675) 
drop spell_n_51919 split

 newspell combine, combine(12 9) id(mbun) ///
stype(drug_num) snumber(spell_n_55675) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_56440) /// 
sort(episode_beg_dt) ncode(56440) 
drop spell_n_55675 split

 newspell combine, combine(12 13) id(mbun) ///
stype(drug_num) snumber(spell_n_56440) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_56831) /// 
sort(episode_beg_dt) ncode(56831) 
drop spell_n_56440 split

 newspell combine, combine(12 18) id(mbun) ///
stype(drug_num) snumber(spell_n_56831) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_35663) /// 
sort(episode_beg_dt) ncode(35663) 
drop spell_n_56831 split

 newspell combine, combine(12 19) id(mbun) ///
stype(drug_num) snumber(spell_n_35663) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_3038) /// 
sort(episode_beg_dt) ncode(3038) 
drop spell_n_35663 split

//The specified spelltypes do not overlap!
newspell combine, combine(14 23) id(mbun) ///
stype(drug_num) snumber(spell_n_3038) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_39709) /// 
sort(episode_beg_dt) ncode(39709) 
drop spell_n_3038 split

//15 does not exist
newspell combine, combine(15 12) id(mbun) ///
stype(drug_num) snumber(spell_n_3038) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_10340) /// 
sort(episode_beg_dt) ncode(10340) 
drop spell_n_3038 split
//15 does not exist
newspell combine, combine(15 13) id(mbun) ///
stype(drug_num) snumber(spell_n_10340) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_36945) /// 
sort(episode_beg_dt) ncode(36945) 
drop spell_n_10340 split

//The specified spelltypes do not overlap!
 newspell combine, combine(17 18) id(mbun) ///
stype(drug_num) snumber(spell_n_3038) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_16591) /// 
sort(episode_beg_dt) ncode(16591) 
drop spell_n_3038 split

 newspell combine, combine(18 1) id(mbun) ///
stype(drug_num) snumber(spell_n_3038) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_25297) /// 
sort(episode_beg_dt) ncode(25297) 
drop spell_n_3038 split

 newspell combine, combine(18 9) id(mbun) ///
stype(drug_num) snumber(spell_n_25297) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_20766) /// 
sort(episode_beg_dt) ncode(20766) 
drop spell_n_25297 split

 newspell combine, combine(18 13) id(mbun) ///
stype(drug_num) snumber(spell_n_20766) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_24179) /// 
sort(episode_beg_dt) ncode(24179) 
drop spell_n_20766 split

 newspell combine, combine(18 19) id(mbun) ///
stype(drug_num) snumber(spell_n_24179) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_48657) /// 
sort(episode_beg_dt) ncode(48657) 
drop spell_n_24179 split

 
newspell combine, combine(19 1) id(mbun) ///
stype(drug_num) snumber(spell_n_48657) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_53025) /// 
sort(episode_beg_dt) ncode(53025) 
drop spell_n_48657 split

 newspell combine, combine(19 13) id(mbun) ///
stype(drug_num) snumber(spell_n_53025) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_27704) /// 
sort(episode_beg_dt) ncode(27704) 
drop spell_n_53025 split

newspell combine, combine(21 1) id(mbun) ///
stype(drug_num) snumber(spell_n_27704) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_38602) /// 
sort(episode_beg_dt) ncode(38602) 
drop spell_n_27704 split


 newspell combine, combine(21 19) id(mbun) ///
stype(drug_num) snumber(spell_n_38602) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_24908) /// 
sort(episode_beg_dt) ncode(24908) 
drop spell_n_38602 split

 newspell combine, combine(22 1) id(mbun) ///
stype(drug_num) snumber(spell_n_27704) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_38013) /// 
sort(episode_beg_dt) ncode(38013) 
drop spell_n_27704 split

//The specified spelltypes do not overlap!
 newspell combine, combine(22 5) id(mbun) ///
stype(drug_num) snumber(spell_n_38013) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_35045) /// 
sort(episode_beg_dt) ncode(35045) 
drop spell_n_38013 split

 newspell combine, combine(22 18) id(mbun) ///
stype(drug_num) snumber(spell_n_27704) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_13951) /// 
sort(episode_beg_dt) ncode(13951) 
drop spell_n_27704 split

//The specified spelltypes do not overlap!
 newspell combine, combine(22 23) id(mbun) ///
stype(drug_num) snumber(spell_n_13951) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_47162) /// 
sort(episode_beg_dt) ncode(47162) 
drop spell_n_13951 split

 
newspell combine, combine(23 12) id(mbun) ///
stype(drug_num) snumber(spell_n_13951) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_51110) /// 
sort(episode_beg_dt) ncode(51110) 
drop spell_n_13951 split

//The specified spelltypes do not overlap!
 newspell combine, combine(23 13) id(mbun) ///
stype(drug_num) snumber(spell_n_51110) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_28189) /// 
sort(episode_beg_dt) ncode(28189) 
drop spell_n_51110 split

newspell combine, combine(23 18) id(mbun) ///
stype(drug_num) snumber(spell_n_51110) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_n_10022) /// 
sort(episode_beg_dt) ncode(10022) 
drop spell_n_51110 split

//again
newspell combine, combine(23 19) id(mbun) ///
stype(drug_num) snumber(spell_post5) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post6) /// 
sort(episode_beg_dt) ncode(2319) 
drop spell_post5 split

replace drug_num=


replace drug_code="POLY" if drug_num >24
replace drug_desc="POLYPHARMACY" if drug_num>24

//18 April 2021
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only_additional_meds.dta"

//let's check for further overlaps

bysort mbun (episode_beg_dt): gen dr_change = 1 if drug_code[_n]!= drug_code[_n-1] & _n>1
bysort mbun (episode_beg_dt): gen overlap = 1 if dr_change[_n]==1 & dr_change[_n+1]==1 & episode_beg_dt[_n+1]-episode_beg_dt[_n]<=31 & drug_code[_n+1]!="GAP" & drug_code[_n]!="GAP"
bysort mbun (episode_beg_dt): replace overlap = 1 if _n==1 & dr_change[_n+1]==1 & episode_beg_dt[_n+1]-episode_beg_dt[_n]<=31 & drug_code[_n+1]!="GAP" & drug_code[_n]!="GAP"


bysort mbun (episode_beg_dt): replace overlap = 1 if dr_change[_n]==1 & dr_change[_n-1]==1 & episode_beg_dt[_n]-episode_beg_dt[_n-1]<=31 & overlap==. & drug_code[_n-1]!="GAP" & drug_code[_n]!="GAP"
bysort mbun (episode_beg_dt): replace overlap = 1 if dr_change[_n]==1 & dr_change[_n-1]==. & episode_beg_dt[_n]-episode_beg_dt[_n-1]<=31 & overlap==. & drug_code[_n-1]!="GAP" & drug_code[_n]!="GAP"

keep if overlap==1
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_addl_meds_further_drugs_to_combine_Apr_20.dta"

keep mbun drug_num drug_code drug_desc
egen drug_pairs = fill(1 2 1 2)
order mbun drug_pairs


use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only_additional_meds.dta"
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_15_April_2021.dta"
//replace the polypharmacy codes so that they can be resolved to the
//constituent drugs
replace drug_num=7.12 if drug_num==3532
replace drug_num=7.19 if drug_num==7250
replace drug_num=7.22 if drug_num==16497
replace drug_num=8.1 if drug_num==12702
replace drug_num=8.13 if drug_num==27125
replace drug_num=8.18 if drug_num==46014
replace drug_num=8.19 if drug_num==683
replace drug_num=9.13 if drug_num==19629
replace drug_num=9.19 if drug_num==6498
replace drug_num=10.18 if drug_num==12018
replace drug_num=10.19 if drug_num==51919
replace drug_num=12.1 if drug_num==55675
replace drug_num=12.9 if drug_num==56440
replace drug_num=12.13 if drug_num==56831
replace drug_num=12.18 if drug_num==35663
replace drug_num=12.19 if drug_num==3038
replace drug_num=14.23 if drug_num==39709
replace drug_num=15.12 if drug_num==10340
replace drug_num=15.13 if drug_num==36945
replace drug_num=17.18 if drug_num==16591
replace drug_num=18.1 if drug_num==25297
replace drug_num=18.9 if drug_num==20766
replace drug_num=18.13 if drug_num==24179
replace drug_num=18.19 if drug_num==48657
replace drug_num=19.1 if drug_num==53025
replace drug_num=19.13 if drug_num==27704
replace drug_num=21.1 if drug_num==38602
replace drug_num=21.19 if drug_num==24908
replace drug_num=22.1 if drug_num==38013
replace drug_num=22.5 if drug_num==35045
replace drug_num=22.18 if drug_num==13951
replace drug_num=22.23 if drug_num==47162
replace drug_num=23.12 if drug_num==51110
replace drug_num=23.13 if drug_num==28189
replace drug_num=23.18 if drug_num==10022

replace drug_num=23.19 if drug_num==700
replace drug_num=1.13 if drug_num==13169
replace drug_num=4.1 if drug_num==8275
replace drug_num=4.13 if drug_num==2112
replace drug_num=4.18 if drug_num==14583
replace drug_num=4.19 if drug_num==20390
replace drug_num=5.1 if drug_num==10577
replace drug_num=5.4 if drug_num==6322
replace drug_num=5.18 if drug_num==27062
replace drug_num=5.19 if drug_num==45762
replace drug_num=6.7 if drug_num==23196
replace drug_num=6.12 if drug_num==1839
replace drug_num=6.18 if drug_num==8272
replace drug_num=6.19 if drug_num==15822
replace drug_num=6.22 if drug_num==3605


replace drug_num=3.5 if drug_num==406410
replace drug_num=3.12 if drug_num==406420
replace drug_num=4.1 if drug_num==4166930
replace drug_num=4.19 if drug_num==2960230
replace drug_num=5.10 if drug_num==3308410
replace drug_num=5.12 if drug_num==406430
replace drug_num=5.18 if drug_num==375320
replace drug_num=6.5 if drug_num==615710
replace drug_num=6.12 if drug_num==524620
replace drug_num=6.16 if drug_num==524610
replace drug_num=6.18 if drug_num==615720
replace drug_num=7.1 if drug_num==4166920
replace drug_num=7.4 if drug_num==4166910
replace drug_num=7.18 if drug_num==2657710
replace drug_num=7.23 if drug_num==5387810
replace drug_num=8.4 if drug_num==2960210
replace drug_num=8.12 if drug_num==3067210
replace drug_num=8.21 if drug_num==1814110
replace drug_num=8.23 if drug_num==524110
replace drug_num=10.4 if drug_num==4086810
replace drug_num=10.9 if drug_num==4701010
replace drug_num=10.12 if drug_num==3308430
replace drug_num=11.1 if drug_num==1468420
replace drug_num=11.12 if drug_num==74410
replace drug_num=11.13 if drug_num==74420
replace drug_num=11.19 if drug_num==1468410
replace drug_num=16.1 if drug_num==3472520
replace drug_num=16.12 if drug_num==524630
replace drug_num=16.18 if drug_num==3472510
replace drug_num=21.7 if drug_num==5554110
replace drug_num=21.12 if drug_num==5554120
replace drug_num=21.22 if drug_num==1591210
replace drug_num=22.10 if drug_num==5717110
replace drug_num=22.12 if drug_num==1591230



//Now we need to correct the polypharm codes in 
//file: CIHI_SGA_SCZ_merged_pp_15_April_2021.dta

//triplet:
replace drug_num=3.5012 if drug_num==4064

//Now append the additional meds to the longitudinal file below
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_19_April_2021.dta"
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only_additional_meds.dta", ///
generate(_appended) keep(mbun drug_num drug_code drug_desc episode_beg_dt episode_end_dt last_drug_de_wrong ///
episode_beg_dt_last_drug episode_end_dt_last_drug day_ult_k_correct)

drop spell_num
sort mbun episode_beg_dt episode_end_dt
order mbun episode_beg_dt episode_end_dt

//fill in the correct ultimate date
bysort mbun (episode_beg_dt): replace day_ult_k = day_ult_k_correct if day_ult_k==.

//now create the adjusted ultimate date:
bysort mbun (episode_beg_dt): egen day_ult_k_max = max(day_ult_k)
format day_ult_k_max %td

bysort mbun (episode_beg_dt): replace day_ult_k = day_ult_k_max

bysort mbun (episode_beg_dt): egen day_0_max = max(day0)
bysort mbun (episode_beg_dt): egen day_1_max = max(day1)

bysort mbun (episode_beg_dt):replace day0 = day_0_max if day0==.
bysort mbun (episode_beg_dt):replace day1 = day_0_max if day1==.

drop day_ult_k_correct day_ult_k_max day_0_max day_1_max
save, replace

//now let's remove the duplicated records that resulted from additional meds
sort mbun episode_beg_dt _appended
bysort mbun drug_code episode_beg_dt: gen copy = _n

drop if mbun==55127 & (copy==2|copy==3)
drop if mbun==55127 & _appended==0

//this block deletes the records that were replaced
//by adding medications
gsort mbun episode_beg_dt drug_num -_appended
by mbun episode_beg_dt drug_num: gen copy=_n
drop if copy==2

//need to re-run the newspall combine olanz (12) and clz (4)
sort mbun episode_beg_dt 
by mbun: gen spell = _n
order mbun spell

//Now create an indicator of last state for the rehospitalized

//the next line gives the last record for both hosp and censored
bysort mbun (episode_beg_dt _appended): gen last_rec=1 if _n==_N

//the next line flags only the rehospitalized
bysort mbun (episode_beg_dt _appended): gen rehosp=1 if last_rec==1 & _appended==1

//Now take the hospitalization dates of the rehospitalized
//and append to the longitudinal file
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_merged_pp_16_April_2021_rehosp_only_additional_meds.dta", clear

bysort mbun (episode_beg_dt): keep if _n==_N
keep mbun episode_beg_dt episode_end_dt self_harm psychos_non psychos_org schizoph schizaff bipolar episode_beg_dt_last_hosp episode_end_dt_last_hosp
replace episode_beg_dt = episode_beg_dt_last_hosp
replace episode_end_dt = episode_end_dt_last_hosp


gen state = 5
drop episode_beg_dt_last_hosp episode_end_dt_last_hosp
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\temp_rehospitalized_19_aug_for_append.dta", replace

//Now append the file above to the longitudinal file
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_19_April_2021.dta"
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\temp_rehospitalized_19_aug_for_append.dta", generate(_from_rehosp)

save, replace
//create an indicator of how many mental health dx at rehospitalization
egen count_dx = rsum(self_harm psychos_non psychos_org schizoph schizaff bipolar)

//fix the multi_dx people
replace drug_code="self_harm/schizo" if count_dx==3
replace drug_code="self_harm/schizo" if count_dx==2 & self_harm==1 & schizoph==1
replace drug_code="self_harm/bipolar" if count_dx==2 & self_harm==1 & bipolar==1
replace drug_code="schizo" if count_dx==2 & schizoph==1 & psychos_org==1
replace drug_code="schizo" if count_dx==2 & schizoph==1 & bipolar==1

//now do the single dx people
replace drug_code = "schizo" if state==5 & schizoph==1 & drug_code==""
replace drug_code = "bipolar" if state==5 & bipolar==1 & drug_code==""
replace drug_code = "self_harm" if state==5 & self_harm==1 & drug_code==""
replace drug_code = "psychos_non" if state==5 & psychos_non==1 & drug_code==""
replace drug_code = "psychos_org" if state==5 & psychos_org==1 & drug_code==""
replace drug_code = "schizaff" if state==5 & schizaff==1 & drug_code==""

//Now handle the other people with  drug_code=="", state==5, and appended==.
replace drug_code = "schizo" if state==5 & schizoph==1 & drug_code=="" & _appended==.
replace drug_code = "bipolar" if state==5 & bipolar==1 & drug_code=="" & _appended==.
replace drug_code = "self_harm" if state==5 & self_harm==1 & drug_code=="" & _appended==.
replace drug_code = "psychos_non" if state==5 & psychos_non==1 & drug_code=="" & _appended==.
replace drug_code = "psychos_org" if state==5 & psychos_org==1 & drug_code=="" & _appended==.
replace drug_code = "schizaff" if state==5 & schizaff==1 & drug_code==""  & _appended==.

//handle the other mental health disorders
replace drug_code = "other mental dx" if state==5 & drug_code=="" & _from_rehosp==1
replace drug_desc=drug_code if state==5
replace drug_num=-1 if state==5

drop if rehosp==1
save, replace
drop _appended last_drug_de_wrong episode_beg_dt_last_drug episode_end_dt_last_drug day_ult_k_max copy spell rehosp _from_rehosp

bysort mbun (episode_beg_dt): replace day0 = day0[1] 
bysort mbun (episode_beg_dt): replace day1 = day1[1]
bysort mbun (episode_beg_dt): replace day_ult_k = day_ult_k[1] 

drop self_harm- bipolar

by mbun: gen spell = _n
newspell gaps, ncode(GAPA) first(individual) last(individual) id(mbun) stype(drug_desc) snumber(spell) ///
begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post) sort(episode_beg_dt)

replace drug_code = "GAPA" if drug_desc=="GAPA"
replace drug_num = 25 if drug_code=="GAPA"
save, replace

sort mbun episode_beg_dt

//now merge "GAP" and "GAPA"
drop spell
newspell merge, merge("GAP", "GAPA" = "GAP") nstype(drug_desc_post) id(mbun) stype(drug_desc) snumber(spell_post) ///
begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post_m) sort(episode_beg_dt)

bysort mbun (episode_beg_dt): gen replace_this = 1 if drug_desc_post=="GAP" & drug_desc_post[_n+1]=="GAPA"
bysort mbun (episode_beg_dt): replace episode_end_dt = episode_end_dt[_n+1] if drug_desc_post=="GAP" & drug_desc_post[_n+1]=="GAPA"
drop if replace_this==1

replace drug_num=24 if drug_num==25
replace drug_code="GAP" if drug_code=="GAPA"
replace drug_desc="GAP" if drug_desc=="GAPA"
drop spell_post_m spell_post drug_desc_post
save, replace

replace state=3 if drug_num==24
replace state=2 if drug_code=="POLY"
replace state=4 if drug_num==4

bysort mbun (episode_beg_dt): gen last_rec=1 if _n==_N
expand 2 if last_rec==1 & drug_num>-1, gen(expand_flag)
sort mbun episode_beg_dt
replace state = 6 if expand_flag==1
replace episode_beg_dt = day_ult_k if state==6
replace episode_end_dt = . if state==6
replace drug_num=0 if state==6
replace drug_code="NOT REHOSP" if state==6
replace drug_desc="NOT REHOSPITALIZED" if state==6

//20 April 2021
//Need to correct the censor dates for the non-hospitalized
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_19_April_2021.dta"
bysort mbun (episode_beg_dt): egen absorb_state = max(state)
bysort mbun (episode_beg_dt): gen not_rehosp = 1 if absorb_state==6
bysort mbun (episode_beg_dt): replace not_rehosp = 0 if absorb_state==5

//Now take the penultimate records (i.e. last medication record of the non-hospitalized)
//We will take the final row of the NPDUIS records to adjust
//the censor date
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp.dta"
keep if expand_flag==0 & last_rec==1

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_All_Drugs_Treated_Patients.dta"
bysort mbun (episode_beg episode_end): keep if _n==_N
bysort mbun (episode_beg episode_end): keep if _n==_N
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_all_treated_patients_last_meds.dta"

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp_last_drug.dta"
ren episode_beg_dt episode_beg_dt_chk
ren episode_end_dt episode_end_dt_chk
ren drug_code drug_code_chk
ren drug_desc drug_desc_chk
merge 1:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_all_treated_patients_last_meds.dta", keepusing(episode_beg_dt episode_end_dt drug_code drug_desc)

//take note of the mismatches:
gen wrong_med_recs = 1 if episode_beg_dt_chk!=episode_beg_dt | drug_desc_chk!= drug_desc

//flag the people with correct last medications
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp.dta"

//the people with correct med history are here:
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp_with_correct_med_hist.dta"

////Work with this again and see if the discrepancy is with the episode_beg_dt only--this is okay
////For those with discrepant end dates, we have to re-run the procedure of gaps/polypharm etc

gen diff_begin_epis_only = 1 if episode_beg_dt_chk!= episode_beg_dt & episode_end_dt_chk==episode_end_dt

//Merge the people with correct med history
//and those with different begin episodes
//these people do not need revisionist history
use  "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_diff_in_episode_begin_only.dta", keepusing(diff_begin_epis_only)
merge m:m mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp_with_correct_med_hist.dta", keepusing(correct_last_med)

bysort mbun (episode_beg): gen correct_history = 1 if diff_begin_epis_only==1 | correct_last_med==1

//Now work on the not rehospitalized with wrong histories
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_not_rehosp_revise_hist_20_Apr.dta"
merge 1:m mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_All_Drugs_Treated_Patients.dta", keepusing(episode_beg_dt- drug_desc)

//Now work on the Polypharm of those with wrong histories
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_not_rehosp_revise_hist_20_Apr.dta
bysort mbun (episode_beg_dt): gen spell_num=_n
order mbun spell_num

///Simply repeat the newspell commands above, the commands below do not work

//Check for additional overlaps
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_addl_meds_further_drugs_to_combine_Apr_20.dta"
drop dr_change overlap

bysort mbun (episode_beg_dt): gen overlap = 1 if episode_beg_dt - episode_end_dt[_n-1]<=0
bysort mbun (episode_beg_dt): replace overlap =1 if episode_beg_dt[_n+1]-episode_end_dt<=0

//Re-check the people with correct med history*/
//Repeat the code circa line 2150
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp_with_correct_med_hist.dta"
bysort mbun (episode_beg_dt): gen spell_n= _n

//Let's do a rank with POLYPHARMACY superseding any single drug
//do this also for those with correct meds
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_not_rehosp_revise_hist_20_Apr.dta"
gen drug_num_cmb = 100 if drug_num>24
replace drug_num_cmb = drug_num if drug_num_cmb==.


replace drug_num_cmb=100 if drug_num>24
replace drug_num_cmb=100 if drug_num<0
replace drug_code = "POLY" if drug_num_cmb>24

replace drug_desc = "POLYPHARMACY" if drug_num_cmb>24



bysort mbun (episode_beg_dt): gen spell = _n
//first run GAPA procedure again
//the code is in line 3594

newspell rank, rank(100 24, 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23) ///
id(mbun) stype(drug_num_cmb) snumber(spell_n_7007) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post) sort(episode_beg_dt)

//repeat the ranking to get rid of overlaps
newspell rank, rank(100 24,19,12 18, 1, 13, 4, 22, 23, 5, 9, 10, 7, 6,8,14,3,16,21,20,2,11,17,15) ///
id(mbun) stype(drug_num_cmb) snumber(spell_post11) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post12) sort(episode_beg_dt)

//resolve the residula overlap of quetiapine and olanzapine

newspell combine if inlist(mbun, 2390, 7462, 8530 17001, 18068, 23363, 28904, 32010, 39714, 44630, 47027, 49210, 55103),  combine(12 18) id(mbun) ///
stype(drug_num) snumber(spell_post12) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post13) /// 
sort(episode_beg_dt) ncode(-35663) 

replace drug_num=35663 if drug_num<0
replace drug_num_cmb=100 if drug_num>24

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_not_rehosp_revise_hist_20_Apr.dta"

gen duration = episode_end_dt- episode_beg_dt if inlist(mbun, 8530, 28904, 47027, 49210) & inlist(drug_num,12,18) & overlap==1
bysort mbun (episode_beg_dt): drop if overlap==1 & overlap[_n-1]==1 & duration == duration[_n-1]

gen take_this=1 if over==1 & dura>30 & dur<.
replace drug_num = 35663 if take_this==1

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp_with_correct_med_hist.dta"
replace drug_num_cmb=100 if drug_code=="POLY"
replace drug_num_cmb= drug_num if drug_num_cmb==.

bysort mbun (episode_beg_dt): gen overlap = 1 if episode_beg_dt - episode_end_dt[_n-1]<=0
bysort mbun (episode_beg_dt): replace overlap =1 if episode_beg_dt[_n+1]-episode_end_dt<=0

//tomorrow, check the file overlap variable in CIHI_SGA_SCZ_20_April_2021_not_rehosp_with_correct_med_hist.dta 
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp_with_correct_med_hist.dta"
drop if drug_num==0

tab drug_num_cmb if overlap==1, sort
newspell rank, rank(100, 12 19 5 18 1 9 10 22 23) ///
id(mbun) stype(drug_num_cmb) snumber(spell_n_17602) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post) sort(episode_beg_dt)


newspell combine if mbun==33084, combine(12 5) id(mbun) ///
stype(drug_num) snumber(spell_post) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post1) /// 
sort(episode_beg_dt) ncode(33084) 
save, replace

newspell combine if mbun==33084, combine(12 5) id(mbun) ///
stype(drug_num) snumber(spell_post1) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post2) /// 
sort(episode_beg_dt) ncode(-33084) 
save, replace

replace drug_num_cmb=100 if drug_num<0 | drug_num>100

newspell combine if inlist(mbun, 56440,51110,44356,2682), combine(22 1) id(mbun) ///
stype(drug_num) snumber(spell_post2) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post3) /// 
sort(episode_beg_dt) ncode(-221) 
save, replace

newspell combine if inlist(mbun, 56440,51110,44356,2682), combine(12 9) id(mbun) ///
stype(drug_num) snumber(spell_post3) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post4) /// 
sort(episode_beg_dt) ncode(-129) 
replace drug_num_cmb=100 if drug_num<0
save, replace

newspell combine if inlist(mbun, 56440,51110,44356,2682), combine(12 23) id(mbun) ///
stype(drug_num) snumber(spell_post4) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post5) /// 
sort(episode_beg_dt) ncode(-1223) 
replace drug_num_cmb=100 if drug_num<0
save, replace



drop split spell_post4

newspell combine if inlist(mbun, 56440,51110,44356,2682), combine(19 18) id(mbun) ///
stype(drug_num) snumber(spell_post5) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post6) /// 
sort(episode_beg_dt) ncode(-1918) 
replace drug_num_cmb=100 if drug_num<0
save, replace


//Now check the end dates of the not rehosp (CIHI_SGA_SCZ_20_April_2021_not_rehosp_with_correct_med_hist.dta)
/*There are 607 patients who are not rehosp with correct med history
There are 2518 patients who are not rehosp with revised med history
There are 1308 people who may have been rehosp but not for mental health dx
There are 142 hospitalized people for any mental dx.
Total: 4575

Delete the ones with polypharmacy as first row (n=162) file: CIHI_addit_poly_1st_23_April.dta
*/

//The canonical file is: CIHI_All_Drugs_Treated_Patients.dta

**Conclusion: the day_ult_k var is wrong in: CIHI_SGA_SCZ_20_April_2021_not_rehosp_with_correct_med_hist.dta

//The file with both rehospitalized and not rehosp is: CIHI_22_Apr_all_cohort.dta" (n=4574)
//The one with rehosp only is: "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_22_Apri_rehosp_only.dta"
//The one with unique patients rehosp is:  "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_22_Apri_rehosp_only_uniq.dta" (n=142)

//Now do a query to retrieve unmatched patients
use Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_22_Apr_all_cohort.dta"
merge 1:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_22_Apri_rehosp_only_uniq.dta", keepusing(not_rehosp)
ren not rehosp_mental
replace rehosp_mental=1 if _m==3
merge 1:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_not_rehosp_correct_hist_uniq.dta", keepusing(not_rehosp)
replace rehosp_mental=0 if _m==3
replace rehosp_mental =. if _m!=3 & rehosp!=1
merge 1:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_not_rehosp_revised_hist_uniq.dta", keepusing(drug_num_cmb)
replace rehosp_mental=0 if _m==3

//Now retrieve the patients hospitalized but possibly not due to mental (n=1,308)
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_22_Apr_retrieve.dta" 
merge 1:m mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_All_Drugs_Treated_Patients.dta", keepusing(episode_beg_dt- drug_desc)
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_22_Apr_retrieve_longit.dta"

//for the file above repeat the procedure in line 1948 GAPS -> COMBINE -> PP

//Now append the retrieved files to the non-hospitalized
//first check unique n's (these are okay)
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_22_Apr_retrieve_longit.dta"
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_SGA_SCZ_20_April_2021_not_rehosp_with_correct_med_hist.dta", generate(_from_correct_hx)
append using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_not_rehosp_revise_hist_20_Apr.dta", generate(_from_rev_hx)

replace drug_num_cmb=100 if drug_num_cmb>19 & drug_num_cmb < 20

//mark last records per person
bysort mbun (episode_beg_dt): gen last_rec = cond(_n==_N,1,0)
save, replace

by mbun: gen day_ult_k = episode_end_dt if last_rec==1
format day_ult_k %td
gsort mbun -episode_beg_dt
by mbun: replace day_ult_k = day_ult_k[_n-1] if day_ult_k==.


expand 2 if last_rec==1, gen(expand_flag)
sort mbun episode_beg_dt
gen state = 6 if expand_flag==1
replace episode_beg_dt = day_ult_k if state==6
//replace episode_end_dt = . if state==6
replace drug_num=0 if state==6
replace drug_code="NOT REHOSP" if state==6
replace drug_desc="NOT REHOSPITALIZED" if state==6

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_23_Apr_states_longit.dta"

bysort mbun (episode_beg_dt): replace last_rec = cond(_n==_N & _from_rehosp==1, 2, last_rec)
replace state = 5 if last_rec==2

//this person was not treated
drop if mbun==55127
//Now check for initial polypharmacy
bysort mbun (episode_beg_dt): gen polypharm_first = cond(drug_num_cmb==100 & _n==1, 1,0)

by mbun: egen poly_first_person= max(polypharm_first)
keep if poly_first_person==1
by mbun: keep if _n==1
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_addit_poly_1st_23_April.dta"

//Now delete the poly first from analysis file
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_23_Apr_states_longit.dta"
merge m:1 mbun using "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_addit_poly_1st_23_April.dta", keepusing(poly_first_person)
drop if poly_first==1


replace drug_num=0 if drug_code=="NOT REHOSP"
replace drug_num_cmb=0 if drug_num==0
bysort mbun (episode_beg_dt): replace drug_num=-1 if _from_rehosp==1 & _n==_N
replace drug_num_cmb=-1 if drug_num==-1

replace state=.
replace state=1 if !inlist(drug_num_cmb, -1, 0, 4, 24,100)


replace drug_num_cmb = 100 if drug_num_cmb==. & floor(drug_num)!=drug_num
replace drug_num_cmb = drug_num if drug_num_cmb==.


label define gamot -1 "Rehosp mental dx" 0"Not rehospitalized" 1 "ARIPIPRAZOLE" 2 "ASANEPINE" 3 "CHLORPROMAZINE" 4 "CLOZAPINE" 5 "FLUPENTIXOL" 6 "FLUPHENAZINE" 7 "HALOPERIDOL" ///
8 "LEVOMEPROMAZINE" 9 "LITHIUM" 10 "LOXAPINE" 11 "LURASIDONE" 12 "OLANZAPINE" 13 "PALIPERIDONE" 14 "PERPHENAZINE" 15 "PIMOZIDE" 16 "PIPOTIAZINE" 17 "PROCHLORPERAZINE" 18 "QUETIAPINE" ///
19 "RISPERIDONE" 20 "Sulpiride" 21 "TRIFLUOPERAZINE" 22 "ZIPRASIDONE" 23 "ZUCLOPENTHIXOL" 24 "Gap" 100 "Polypharmacy"

label values drug_num_cmb gamot

lab def estado 1"SGA monotherapy" 2"Polypharmacy" 3"Gap" 4"Clozapine" 5"Rehosp mental dx" 6"Not rehospitalized"

replace state=2 if drug_num_cmb==100
replace state=3 if drug_num_cmb==24
replace state=4 if drug_num_cmb==4
replace state=5 if drug_num_cmb==-1
replace state=6 if drug_num_cmb==0

lab values state estado

//Now merge the personal characteristics
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_23_Apr_states_longit.dta"

replace drug_num_cmb=24 if drug_num==24
replace state=3 if drug_num_cmb==24

replace state=2 if drug_num_cmb==100

///////////////////////////////////////
*24 April 2021
*Multistate modelling
///////////////////////////////////////

ssc install multistate
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_23_Apr_states_stata_try.dta"
//create an outcome variable
gen rehosp=0
replace rehosp=1 if state==5

drop if state==6
save, replace
///////////////////////////////////////
*25 April 2021
*Reshape for ultistate modelling
///////////////////////////////////////

///////Reformatting the data to be amenable to mstate in Stata
use"Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_27_Apr_fixed.dta"

gen status = 0 if state!=5
replace status=1 if state==5

replace state=5 if state==6
label values state .
lab def estado5  1"SGA monotherapy" 2"Polypharmacy" 3"Gap" 4"Clozapine" 5"Terminal"
label values state estado5

bysort mbun (episode_beg_dt state status): gen first_or_last =cond(_n==1| _n==_N, 1,0) 
expand 2 if first_or_last==0, gen(expand_flag)

//generate pairs of rows
bysort mbun (episode_beg_dt): egen ctr = seq(), f(1) t(2)
bysort mbun (episode_beg_dt): gen recnum=_n



ren episode_beg_dt epis_dt_
drop episode_end_dt
order mbun recnum ctr
reshape wide state status epis_dt_ time, i(mbun recnum) j(ctr)
bysort mbun (recnum): gen state2_fill = state2[_n+1]



bysort mbun (recnum epis_dt_1 state1): gen epis2_fill = epis_dt_2[_n+1]
format epis2_fill %td

bysort mbun (recnum epis_dt_1 state1): gen status2_fill = status2[_n+1]
bysort mbun (recnum epis_dt_1 state1): gen time2_fill = time2[_n+1]
order mbun recnum epis_dt_1 epis2_fill state1 state2_fill status2_fill time2_fill




drop state2 epis_dt_2 status2 time2

ren epis2_fill epis_dt_2
ren state2_fill state2
ren status2_fill status2
ren time2_fill time2
label values state2  estado5
drop if state1==.

drop status1
ren status2 status
drop recnum first_or_last expand_flag

gen time = time2-time1

//rename to avoid confusion
ren state1 state_x
ren state2 state_y

lab def estado5  1"SGA monotherapy" 2"Polypharmacy" 3"Gap" 4"Clozapine" 5"Terminal"
lab values state_x state_y estado5

save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_28_Apr_states_stata_msm.dta "
///Take the covariates





/////////Try Crowther again
use  "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_25_Apr_states_stata_msm.dta ""

msset , id(mbun) states(state_x state_y) times(episode_beg_dt episode_end_dt)
format _start _stop %td

mat tmat = r(transmatrix)

replace _stop = _start+1 if _stop==.

stset _stop, enter(_start) failure(_status==1) scale(30)

/////////////////////////////Here, combine the consecutive states that are the same////////
///Also check for polypharmacy first 
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_26_Apr_fix_CLZ_merge_combine.dta"
bysort mbun (episode_beg_dt): gen flag = 1 if state==2 & _n==1
by mbun: egen pp_max = max(flag)
drop if pp==1
replace state = 4 if drug_num_cmb==4
//Now to merge the spells of the same kind, use line 2000 as a template

gen new_episode_beg_dt = .
gen new_episode_end_dt = .


//sort by state so the adjacent ones can be merged
sort mbun state episode_beg_dt


by mbun: gen merge=1 if state==state[_n-1] & episode_beg_dt-episode_end_dt[_n-1] <= 1

order mbun episode_beg_dt- new_episode_end_dt merge
sort mbun state episode_beg_dt
replace new_episode_beg_dt = episode_beg_dt if merge==.
//replace new_episode_end_dt = episode_end_dt if merge==.

//take the very last record for each drug as the end date
gsort mbun state -episode_end_dt
by mbun state: replace new_episode_end_dt = episode_end_dt if _n==1 | (merge==1 & merge[_n-1]==.)
//this fills in the end dates with the latest in a series
by mbun: replace new_episode_end_dt=new_episode_end_dt[_n-1] if merge==1 & merge[_n-1]!=.

//fill in the begin dates
sort mbun state episode_beg_dt
by mbun state: replace new_episode_beg_dt = new_episode_beg_dt[_n-1] if merge==1 & new_episode_beg_dt==.

//drop the leading records in a group to be merged
bysort mbun state new_episode_beg_dt: drop if new_episode_end_dt==. & merge==. & new_episode_end_dt[_n+1]<.
//assign new end dates if still missing
replace new_episode_end_dt = episode_end_dt if new_episode_end_dt==. & merge==.

duplicates tag mbun state new_episode_beg_dt new_episode_end_dt, gen(dup_rows)
order mbun state drug_desc new_episode_beg_dt new_episode_end_dt merge dup_rows

bysort mbun state new_episode_beg_dt new_episode_end_dt dup_rows: keep if _n==1

sort mbun new_episode_beg_dt state
format new_episode_beg_dt %td
format new_episode_end_dt %td
drop episode_beg_dt episode_end_dt
ren new_episode_beg_dt episode_beg_dt
ren new_episode_end_dt episode_end_dt
drop merge dup_rows 

bysort mbun (episode_beg_dt): gen spell = _n


//Combine common states
newspell rank, rank(5 6 3, 2, 4 1) ///
id(mbun) stype(state) snumber(spell) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post) sort(episode_beg_dt)

//overlap check
bysort mbun (episode_beg_dt): gen overlap = 1 if episode_beg_dt - episode_end_dt[_n-1]<=0
bysort mbun (episode_beg_dt): replace overlap =1 if episode_beg_dt[_n+1]-episode_end_dt<=0

//Fix the censor dates of those not rehospitalized
bysort mbun (episode_beg_dt state): replace episode_beg_dt= episode_beg_dt+1 if _n==_N & state==6
bysort mbun (episode_beg_dt state): replace episode_end_dt= episode_end_dt+1 if _n==_N & state==6

//there are remaining 533 overlaps after the above procedure
//Overlap procedure does not catch "contained" records

*case 1: subseq rec is contained
bysort mbun (episode_beg_dt state): gen contained = 1 if episode_beg_dt==episode_beg_dt[_n-1] & episode_end_dt < episode_end_dt[_n-1]

*case 2: preced rec is contained
bysort mbun (episode_beg_dt state): replace contained = 1 if episode_beg_dt == episode_beg_dt[_n+1] & episode_end_dt < episode_end_dt[_n+1]

//Now turn the SGA mono into poly if prev and succeeding recs are SGAs

*case 1 //take note that state is restricted to 1
bysort mbun (episode_beg_dt state): gen replace_end = episode_end_dt[_n-1] if overlap==1 & overlap[_n-1]==1 & contained==1 & state==1 & state[_n-1]==1 & state==1
format replace_end %td

*case 2 //take note that state is restricted to 1
bysort mbun (episode_beg_dt state): replace replace_end = episode_end_dt[_n+1] if overlap==1 & overlap[_n+1]==1 & contained==1 & state==1 & state[_n+1]==1 & state==1

//carry out the replace drug_cmb, drug_desc, drug_code, state
replace episode_end_dt = replace_end if replace_end < . 
replace drug_desc="POLYPHARMACY" if replace_end <.
replace state=2 if replace_end <. 
replace drug_num_cmb = 100 if replace_end <.



//create an indicator of record to drop

gen drop_this =1 if replace_end ==. & replace_end[_n-1]<. & overlap==1 & overlap[_n-1]==1 & episode_end_dt > episode_end_dt[_n-1]
bysort mbun (episode_beg_dt state): replace drop_this =1 if replace_end ==. & replace_end[_n+1]<. & overlap==1 & overlap[_n+1]==1 & episode_end_dt > episode_end_dt[_n+1]


//create string of drug number
bysort mbun (episode_beg_dt state): gen cc_drug_num=string(drug_num) +"." + string(drug_num[_n-1]) if replace_end <. & drop_this[_n-1]==1
bysort mbun (episode_beg_dt state): replace cc_drug_num=string(drug_num) +"." + string(drug_num[_n+1]) if replace_end <. & drop_this[_n+1]==1

replace cc_drug_num="13.12" if cc_drug_num=="13.12.13"
replace cc_drug_num="18.19" if cc_drug_num=="18.18.19"

destring cc_drug_num, replace


drop if drop_this==1
drop drop_this


///Now handle the overlapping GAPS


*case 1: subseq rec is contained
bysort mbun (episode_beg_dt state): gen contained = 1 if episode_beg_dt==episode_beg_dt[_n-1]  & episode_end_dt < episode_end_dt[_n-1]

*case 2: preced rec is contained
bysort mbun (episode_beg_dt state): replace contained = 1 if episode_beg_dt == episode_beg_dt[_n+1] & episode_end_dt < episode_end_dt[_n+1]

//Now consolidate the gaps

*case 1 //take note that state is restricted to 3
bysort mbun (episode_beg_dt state): gen replace_end = episode_end_dt[_n-1] if overlap==1 & overlap[_n-1]==1 & contained==1 & state==3 & state[_n-1]==3 
format replace_end %td

*case 2 //take note that state is restricted to 3
bysort mbun (episode_beg_dt state): replace replace_end = episode_end_dt[_n+1] if overlap==1 & overlap[_n+1]==1 & contained==1 & state==3 & state[_n+1]==3 

//create an indicator of record to drop

gen drop_this =1 if replace_end ==. & replace_end[_n-1]<. & overlap==1 & overlap[_n-1]==1 & episode_end_dt > episode_end_dt[_n-1]
bysort mbun (episode_beg_dt state): replace drop_this =1 if replace_end ==. & replace_end[_n+1]<. & overlap==1 & overlap[_n+1]==1 & episode_end_dt > episode_end_dt[_n+1]

replace episode_end_dt = replace_end if replace_end < .

drop if drop==1

//combine the PP's

*case 1: subseq rec is contained
bysort mbun (episode_beg_dt state): gen contained = 1 if episode_beg_dt==episode_beg_dt[_n-1]  & episode_end_dt < episode_end_dt[_n-1]

*case 2: preced rec is contained
bysort mbun (episode_beg_dt state): replace contained = 1 if episode_beg_dt == episode_beg_dt[_n+1] & episode_end_dt < episode_end_dt[_n+1]

//Create a replace variable
*case 1 //take note that state is restricted to 2
bysort mbun (episode_beg_dt state): gen replace_end = episode_end_dt[_n-1] if overlap==1 & overlap[_n-1]==1 & contained==1 & state==2 & state[_n-1]==2
format replace_end %td

*case 2 //take note that state is restricted to 2
bysort mbun (episode_beg_dt state): replace replace_end = episode_end_dt[_n+1] if overlap==1 & overlap[_n+1]==1 & contained==1 & state==2 & state[_n+1]==2

//create an indicator of record to drop

gen drop_this =1 if replace_end ==. & replace_end[_n-1]<. & overlap==1 & overlap[_n-1]==1 & episode_end_dt > episode_end_dt[_n-1]
bysort mbun (episode_beg_dt state): replace drop_this =1 if replace_end ==. & replace_end[_n+1]<. & overlap==1 & overlap[_n+1]==1 & episode_end_dt > episode_end_dt[_n+1]

replace episode_end_dt = replace_end if replace_end < .

drop if drop==1



//Combine common states
newspell rank, rank(5 6 3, 2, 4, 1) ///
id(mbun) stype(state) snumber(spell) begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_post) sort(episode_beg_dt)


//Finalize
bysort mbun (episode_beg_dt): gen init_dur=episode_end_dt - episode_beg_dt if _n==1
bysort mbun (episode_beg_dt): gen pp_first= 1 if state==2 & _n==1
bysort mbun (episode_beg_dt): egen max_pp = max(pp_first)
bysort mbun (episode_beg_dt): egen max_in = max(init_dur)
drop if max_pp==1

//drop people who did not have at least 6 weeks of SGA monotherapy
drop if max_in<42

//create variable of initial SGA treatment
by mbun (episode_beg_dt): gen init_SGA_dur = episode_end_dt-episode_beg_dt if _n==1

//check if there are duplicated states to be merged
bysort mbun (episode_beg_dt): gen merge = 1 if state == state[_n-1]

expand 2 if mbun==3753 & episode_beg_dt==date("June 1, 2014", "MDY"), gen(exp)

bysort mbun (episode_beg_dt): gen spell = _n
newspell gaps, ncode(7) first(individual) last(individual) id(mbun) stype(state) snumber(spell) ///
begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_gap_post) sort(episode_beg_dt)

newspell merge, merge(7 2=2) nstype(state_new) id(mbun) stype(state) snumber(spell_gap_post) ///
begin(episode_beg_dt) end(episode_end_dt) newsnumber(spell_mrg_post) sort(episode_beg_dt)


////////////////////////////////////////////////////////////
///28 April 2021/////////////
///Try Crowther again here.
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_28_Apr_Crowther.dta"
drop drug_desc drug_num_cmb drug_code

bysort mbun (episode_beg_dt): gen first_rec = 1 if _n==1
expand 2 if first_rec==1, gen(d1)
replace episode_beg_dt = day1 if d1==1
bysort mbun (episode_beg_dt): replace episode_end_dt = episode_beg_dt[_n+1]-1  if d1==1
replace state = 0 if d1==1

lab def esta 0"Discharge" 1"SGA monotherapy" 2"Polypharmacy" 3"Gap" 4"Clozapine" 5"Rehosp mental dx" 6"Not rehospitalized"
label values state esta


////////////////////////////////////////////////////////////
//3 May 2021
//Exclude people with ages 18 and below
////////////////////////////////////////////////////////////
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_28_Apr_Crowther.dta"
drop if age <19
save "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_3_May_Crowther_adults.dta"

gen status = cond(state==5,1,0)
recode state (0=0) (1=1) (2=2)(3=3)(4=4) (5/6=5), gen(state_new)
drop state
ren state_new state

lab def estados 0"Discharge" 1"SGA monotherapy" 2"Polypharmacy" 3"Gap" 4"Clozapine" 5"Terminal"
lab values state estados
matrix define tmat = (.,1,.,.,.,.\., ., 2,3,4,5\.,6,.,7,8,9\.,10,11,.,12,13\.,14,15,16,.,17\.,.,.,.,.,.)
matrix rownames tmat = "Discharge" "SGA monotherapy" "Polypharmacy" "Gap" "Clozapine" "Terminal"
matrix colnames tmat = "Discharge" "SGA monotherapy" "Polypharmacy" "Gap" "Clozapine" "Terminal"
matrix list tmat

gen trans = .
bysort mbun (episode_beg_dt): replace trans=1 if state 

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_3_May_Crowther_adults_reshaped.dta"
gen _from = state1
gen _to = state2
gen _start = epis_dt_1
gen _stop = epis_dt_2
format _start _stop %td

gen _trans = 1 if state1==0 & state2==1
replace _trans = 2 if state1==1 & state2==2
replace _trans = 3 if state1==1 & state2==3
replace _trans = 4 if state1==1 & state2==4
replace _trans = 5 if state1==1 & state2==5
replace _trans = 6 if state1==2 & state2==1
replace _trans = 7 if state1==2 & state2==3
replace _trans = 8 if state1==2 & state2==4
replace _trans = 9 if state1==2 & state2==5
replace _trans = 10 if state1==3 & state2==1
replace _trans = 11 if state1==3 & state2==2
replace _trans = 12 if state1==3 & state2==4
replace _trans = 13 if state1==3 & state2==5
replace _trans = 14 if state1==4 & state2==1
replace _trans = 15 if state1==4 & state2==2
replace _trans = 16 if state1==4 & state2==3
replace _trans = 17 if state1==4 & state2==5

gen _status= status

stset _stop, enter(_start) failure(_status==1) scale(30)

bysort mbun (epis_dt_1): gen init_dr = drug_num if _n==1
bysort mbun (epis_dt_1): egen max_init = max(init_dr)

replace max_init = 99 if inlist(max_init, 6,10, 22)
label define una 1"ARIPIPRAZOLE" 12 "OLANZAPINE" 13 "PALIPERIDONE" 18 "QUETIAPINE" 19 "RISPERIDONE" 22"ZIPRASIDONE"
rename max_init first_drug
label values first_drug una

//create dummy vars for the drugs:
gen arip = cond(first_drug==1, 1,0)
gen olan = cond(first_drug==12, 1,0)
gen pali = cond(first_drug==13, 1,0)
gen quet = cond(first_drug==18,1,0)
gen risp = cond(first_drug==19,1,0)
gen other = cond(first_drug==99,1,0)

stcox age  female rural_unk ib(last).first_drug 
gen clz = 0 if state2!=4
replace clz=1 if state2==4

//Revert to Apr 27 fixed to get rid of State 0
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_27_Apr_fixed.dta"
drop if age <19
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_4_May_adults_6_SGAs.dta"
bysort mbun (episode_beg_dt): gen first_rec = 1 if _n==1

/*risperidone (Risperdal)
quetiapine (Seroquel)
olanzapine (Zyprexa)
ziprasidone (Zeldox
paliperidone (Invega)
aripiprazole (Abilify)*/ 

gen to_drop=1 if first_rec==1 & drug_num==6
replace to_drop=1 if first_rec==1 & drug_num==10
bysort mbun (episode_beg_dt): egen max_drop = max(to_drop)

bysort mbun (episode_beg_dt): gen init_dr = drug_num if first_rec==1
bysort mbun (episode_beg_dt): egen max_init = max(init_dr)

bysort mbun (episode_beg_dt): gen delay= episode_beg_dt-day1 if _n==1
bysort mbun (episode_beg_dt): egen treat_delay = max(delay)

gen status = cond(state==5,1,0)
recode state (5/6=5), gen(new_state)
lab def unidos 1"SGA monotherapy" 2"Polypharmacy" 3"Gap" 4"Clozapine" 5"Terminal"
lab values state unidos

//create a time variable
bysort mbun (episode_beg_dt): gen time = episode_beg_dt - episode_beg_dt[1]

use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_4_May_adults_6_SGA_6states.dta"
gen state6 = state
replace state6=6 if status==1
replace state6 = . if state==5
replace state6= 5 if state==5 & status==0
replace state6=6 if state==5 & status==1

//2 June 2021
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\CIHI_4_May_adults_6_SGA_6states.dta"
gen months = time/30

bysort mbun(episode_beg_dt)

//Demographics

bysort province: summ age
tab female province, col
tab first_drug province, col
tab rural_unk province, col
bysort province: summ treat_delay

//Now take the maximum follow-up time
use "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\Demogs\CIHI_Schizoph_Cohort_8_June_2021_max_followup.dta"

gen foll_up_months = (episode_beg_dt- day1)/30


/*August 23, 2021
This part addresses Evyn's comment regarding the underestimate of CLZ.
We disaggregate the PP patients into CLZ and other meds.
*/

//The following file contains the drug data before polypharmacy became a category
//Drop the people who should not be part of the 2,997
use "C:\Users\llb296\CLZ\PP_CLZ_split\CIHI_10_Apr_drugs_not_PP_consol.dta"

merge m:1 mbun using "C:\Users\llb296\CLZ\PP_CLZ_split\CIHI_3_June_2021_unique.dta", keepusing(ln_age)
drop if _m==1

//Now create a unique list of people who were ever on PP or CLZ
//PP = state 2
//CLZ = state 4

use "C:\Users\llb296\CLZ\PP_CLZ_split\CIHI_3_June_2021.dta"

/*
replace drug_num = 1 if drug_desc=="ARIPIPRAZOLE"
replace drug_num = 2 if drug_desc=="ASANEPINE"
replace drug_num = 3 if drug_desc=="CHLORPROMAZINE"
replace drug_num = 4 if drug_desc=="CLOZAPINE"
replace drug_num = 5 if drug_desc=="FLUPENTIXOL"
replace drug_num = 6 if drug_desc=="FLUPHENAZINE"
replace drug_num = 7 if drug_desc=="HALOPERIDOL"
replace drug_num = 8 if drug_desc=="LEVOMEPROMAZINE"
replace drug_num = 9 if drug_desc=="LITHIUM"
replace drug_num = 10 if drug_desc=="LOXAPINE"
replace drug_num = 11 if drug_desc=="LURASIDONE"
replace drug_num = 12 if drug_desc=="OLANZAPINE"
replace drug_num = 13 if drug_desc=="PALIPERIDONE"
replace drug_num = 14 if drug_desc=="PERPHENAZINE"
replace drug_num = 15 if drug_desc=="PIMOZIDE"
replace drug_num = 16 if drug_desc=="PIPOTIAZINE"
replace drug_num = 17 if drug_desc=="PROCHLORPERAZINE"
replace drug_num = 18 if drug_desc=="QUETIAPINE"
replace drug_num = 19 if drug_desc=="RISPERIDONE"
replace drug_num = 20 if drug_desc=="Sulpiride"
replace drug_num = 21 if drug_desc=="TRIFLUOPERAZINE"
replace drug_num = 22 if drug_desc=="ZIPRASIDONE"
replace drug_num = 23 if drug_desc=="ZUCLOPENTHIXOL"
replace drug_num=24 if drug_desc=="GAP"
*/

drop if inlist(drug_num, 1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24)
//drop rehosp
drop if drug_num==0
drop if drug_num==-1
save "C:\Users\llb296\CLZ\PP_CLZ_split\CIHI_3_June_2021_longit_POLY_CLZ_only.dta", replace


//re-do this part from CIHI_3_June_2021.dta
replace drug_num = 12.19 if drug_num==3038
replace drug_num = 13.19 if drug_num==27704
replace drug_num = 4.7 if drug_num==-4166910
replace drug_num = 1.19 if drug_num==-53025
replace drug_num = 8.18 if drug_num==-46014 
replace drug_num = 8.13 if drug_num==-27125
replace drug_num = 1.11 if drug_num==111
replace drug_num = 9.11 if drug_num==119
replace drug_num = 4.10 if drug_num==410
replace drug_num = 18.22 if drug_num==766
replace drug_num = 8.19 if drug_num==819
replace drug_num = 12.19 if drug_num==987
replace drug_num = 12.13 if drug_num==1298
replace drug_num = 11.13 if drug_num==1311
replace drug_num = 4.12 if drug_num==1326
replace drug_num = 13.18 if drug_num==1447
replace drug_num = 19.23 if drug_num==1923
replace drug_num = 18.19 if drug_num==2562
replace drug_num = 12.22 if drug_num==2623
replace drug_num = 7.8 if drug_num==2628
replace drug_num = 12.23 if drug_num==2693
replace drug_num = 8.9 if drug_num==4005
replace drug_num = 12.23 if drug_num==4559
replace drug_num = 7.10 if drug_num==6102
replace drug_num = 1.18 if drug_num==8859
replace drug_num = 1.12 if drug_num==10463
replace drug_num = 1.10 if drug_num==12894
replace drug_num = 12.18 if drug_num==17602
replace drug_num = 5.18 if drug_num==18068
replace drug_num = 1.19 if drug_num==24311
replace drug_num = 13.19 if drug_num==27763
replace drug_num = 1.13 if drug_num==37649
replace drug_num = 1.23 if drug_num==231111
replace drug_num = 12.23 if drug_num==231212
replace drug_num = 18.19 if drug_num==25622562
replace drug_num = 1.22 if drug_num==-221

replace drug_desc = "LEVOMEPROMAZINE/LURASIDONE/RISP" if drug_num==8119


/*See also the part above, where the word *constituent* appears*/

//Now make sure the lower codes go first
replace drug_num=1.19 if drug_num==19.1
replace drug_num=1.21 if drug_num==21.1


replace drug_num=9.18 if drug_num==18.9
replace drug_num=13.19 if drug_num==19.13

replace drug_num=19.21 if drug_num==21.19
replace drug_num=1.22 if drug_num==22.1
replace drug_num=12.22 if drug_num==22.12
replace drug_num=1.19 if drug_num==19.1
replace drug_num=13.19 if drug_num==19.13

replace drug_num=4.5 if drug_num==5.4
replace drug_num=4.7 if drug_num==7.4
replace drug_num=1.5 if drug_num==5.1
replace drug_num=1.7 if drug_num==7.1

replace drug_num=18.22 if drug_num==22.18
replace drug_num=12.22 if drug_num>22.12 & drug_num <22.13

replace drug_num=5.22 if drug_num==22.5
replace drug_num=1.22 if drug_num>22.09 & drug_num <22.11

replace drug_num = 1.18 if drug_num>18.09 & drug_num <18.11
replace drug_num = 1.19 if drug_num>19.09 & drug_num <19.11
replace drug_num = 4.10 if drug_num>10.39 & drug_num <10.41
replace drug_num = 9.10 if drug_num>10.89 & drug_num <10.91
replace drug_num = 9.12 if drug_num>12.89 & drug_num <12.91
replace drug_num = 1.12 if drug_num>12.09 & drug_num <12.11
replace drug_num = 12.15 if drug_num>15.11 & drug_num <15.13
replace drug_num = 13.18 if drug_num>18.12 & drug_num <18.14
replace drug_num = 9.18 if drug_num>18.89 & drug_num <18.91
replace drug_num = 13.19 if drug_num>19.12 & drug_num <19.14
replace drug_num = 18.22 if drug_num>22.17 & drug_num <22.19
replace drug_num = 12.23 if drug_num>23.11 & drug_num <23.13
replace drug_num = 13.23 if drug_num>23.12 & drug_num <23.14
replace drug_num = 18.23 if drug_num>23.17 & drug_num <23.19

replace drug_num = 19.23 if drug_num>23.18 & drug_num <23.20
replace drug_num = 1.9 if drug_num > 9.0 & drug_num < 9.11

gen drug_num_rnd = round(drug_num, .01)
replace drug_num = drug_num_rnd
drop drug_num_rnd

/*24 Aug 2021
Now keep only the 78 recs who were labeled POLY
These will be re-assigned to CLZ in the R file in T460s llb296*/

use "C:\Users\llb296\CLZ\PP_CLZ_split\CIHI_3_June_2021_longit_POLY_CLZ_only.dta"
keep if drug_num_rnd >4.000 & drug_num_rnd < 4.99
gen drug_desc_temp = "CLOZAPINE"
drop drug_desc
gen state_temp = 4
gen state5_temp = 4
drop state state5
drop drug_num_cmb-drug_num_rnd
saveold "Y:\Clozapine_from_CIHI\Lloyd_Folder\MSM\slurm\PP_to_CLZ_78_recs.dta", version(12)
save "C:\Users\llb296\CLZ\PP_CLZ_split\CLZ_24_Aug_PP_CLZ_78_recs.dta"


/*Let's also update the drug_num in the main analysis file in Stata*/
use "C:\Users\llb296\CLZ\PP_CLZ_split\CIHI_3_June_2021.dta"
save "C:\Users\llb296\CLZ\PP_CLZ_split\CIHI_24_Aug_2021.dta", replace


replace drug_desc = drug_desc_temp if drug_desc_temp!=""
replace state = state_temp if state_temp <.
replace state5 = state5_temp if state5_temp <.
drop state_temp drug_desc_temp state5_temp _merge
save, replace

//here calculate the mean time to clozapine initiation
use "C:\Users\llb296\CLZ\PP_CLZ_split\cohort_members_ever_clz.dta"
sort mbun episode_beg_dt

by mbun: gen time_to_clz = (episode_beg_dt - day1)/30 if _n==1