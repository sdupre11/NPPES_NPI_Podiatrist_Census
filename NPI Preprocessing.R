# The earliest day that someone might appear in the data is May 23, 2005 as this was when CMS began accepting applications for NPIs
# NPIs were required for HIPAA-covered transactions as of May 23, 2007

library(tidyverse)

#####################################################################################
#### Data Import#####################################################################
#####################################################################################
NPI_chunk_1 <- readRDS("C:/Users/sdupre.APMA/OneDrive - APMA/Desktop/Reference Materials/Data/NPI Registry/processed/NPI_20050523-20250608_chunk_01.rds")
NPI_chunk_1 %>% 
  glimpse()


## Restrict to anyone who has ever used a podiatric Taxonomy code
podiatristTaxoCodes <- c("213ES0103X", # Foot & Ankle Surgery
                         "213ES0131X", # Foot Surgery
                         "213EP1101X", # Primary Podiatric Medicine
                         "213EP0504X", # Public Medicine
                         "213ER0200X", # Radiology
                         "213ES0000X", # Sports Medicine
                         "213E00000X", # Deactivated, Podiatrist
                         "213EG0000X" # Deactivated, Podiatrist
                         #Eliminated the code for Podiatric Assistants: 211D00000X. These are not DPMs.
                         )

## Classifying those who have moved to foot/ankle-associated orthopod taxonomic codes
orthopaedicTaxoCodes <- c("207XS0114X", # Adult Reconstructive Orthopaedic Surgery Physician
                          "207XX0004X", # Orthopaedic Foot and Ankle Surgery Physician
                          "207XP3100X", # Pediatric Orthopaedic Surgery Physician
                          "207XX0005X", # Sports Medicine (Orthopaedic Surgery) Physician
                          ) 

taxonomy_code_cols <- paste0("Healthcare Provider Taxonomy Code_", 1:15)

taxonomy_code_cols <- taxonomy_code_cols[taxonomy_code_cols %in% colnames(NPI_chunk_1)]



NPI_chunk_1 %>%
  filter(`Entity Type Code`== 1) %>%
  filter(!is.na(`Is Sole Proprietor`)) %>% #All cases in the first million records where this field and the `Entity Type Code` field were both NAs were all deactivated nearly-entirely-empty NPI records.
  filter(First_Podiatrist_Taxonomy_Num <= 2) %>%
  mutate(
    Last_Taxonomy_Code = pmap_chr(
      select(., all_of(taxonomy_code_cols)),
      ~ {
        vals <- c(...)                 # All values across the 15 fields for this row
        last_val <- tail(na.omit(vals), 1)  # Get the last non-NA value
        if (length(last_val) == 0) NA_character_ else last_val
      }
    ),
    Last_Taxonomy_Code_Num = pmap_int(
      select(., all_of(taxonomy_code_cols)),
      ~ {
        vals <- c(...)
        non_na_idxs <- which(!is.na(vals))
        if (length(non_na_idxs) == 0) NA_integer_ else tail(non_na_idxs, 1)
      }
    ),
    Has_Podiatrist_Taxonomy = pmap_lgl(
      select(., all_of(taxonomy_code_cols)),
      ~ any(c(...) %in% podiatristTaxoCodes)
    ),
    Has_Podiatrist_Taxonomy = if_else(Has_Podiatrist_Taxonomy, "Yes", "No"),
    First_Podiatrist_Taxonomy_Num = pmap_int(
      select(., all_of(taxonomy_code_cols)),
      ~ {
        vals <- c(...)
        match_idxs <- which(vals %in% podiatristTaxoCodes)
        if (length(match_idxs) == 0) NA_integer_ else match_idxs[1]
      }
    ),
    Last_Podiatrist_Taxonomy_Num = pmap_int(
      select(., all_of(taxonomy_code_cols)),
      ~ {
        vals <- c(...)
        match_idxs <- which(vals %in% podiatristTaxoCodes)
        if (length(match_idxs) == 0) NA_integer_ else tail(match_idxs, 1)
      }
    ),
    Left_Profession_Flag = case_when(
      (Last_Taxonomy_Code_Num - Last_Podiatrist_Taxonomy_Num) > 0 ~ "Yes",
      TRUE ~ "No"
    ),
    Became_Orthopod_Flag = case_when(
      (Last_Taxonomy_Code %in% orthopaedicTaxoCodes) ~ "Yes",
      TRUE ~ "No"
    ),
    `NPI Deactivation Date` = mdy(`NPI Deactivation Date`),
    `NPI Deactivation Year` = as.numeric(year(`NPI Deactivation Date`)),
    `NPI Deactivation Month` = month(`NPI Deactivation Date`),
    `NPI Reactivation Date` = mdy(`NPI Reactivation Date`),
    `NPI Reactivation Year` = as.numeric(year(`NPI Reactivation Date`)),
    `NPI Reactivation Month` = month(`NPI Reactivation Date`),
    `Provider Enumeration Date` = mdy(`Provider Enumeration Date`),
    `Provider Enumeration Year` = as.numeric(year(`Provider Enumeration Date`)),
    `Provider Enumeration Month` = month(`Provider Enumeration Date`),
    `Last Update Date` = mdy(`Last Update Date`),
    `Last Update Year` = as.numeric(year(`Last Update Date`)),
    `Last Update Month` = month(`Last Update Date`),
    `Certification Date` = mdy(`Certification Date`),
    `Certification Year` = as.numeric(year(`Certification Date`)),
    `Certification Month` = month(`Certification Date`)
  ) %>%
  filter(Has_Podiatrist_Taxonomy == "Yes") %>%
  mutate(
    Provider_Status = case_when(
      () ~ "Current",
      () ~ "Retired", 
      () ~ "Changed Fields",
      (Became_Orthopod_Flag == "Yes") ~ "Changed Fields and Became Orthopod",
      () ~ "Dead",
      (!is.na(`Replacement NPI`)) ~ "CHECK: None NA Replacement NPI is present", 
      TRUE ~ "CHECK: TBD"
    )) %>%
  # select(NPI,
  #        `Provider Last Name (Legal Name)`,
  #        `Provider First Name`,
  #        `Provider Middle Name`,
  #        `Provider Credential Text`,
  #        `Provider First Line Business Mailing Address`,
  #        `Provider Second Line Business Mailing Address`,
  #        `Provider Business Mailing Address City Name` ,
  #        `Provider Business Mailing Address State Name` ,
  #        `Provider Business Practice Location Address Postal Code`,
  #        `Provider Business Practice Location Address Country Code (If outside U.S.)`,
  #        `Provider Enumeration Date`,
  #        `Last Update Date`,
  #        `NPI Deactivation Reason Code`,
  #        `NPI Deactivation Date`,
  #        `NPI Deactivation Date`,
  #        `NPI Reactivation Date`,
  #        `Provider Sex Code` ,
  #        `Healthcare Provider Taxonomy Code_1`,
  #        `Certification Date`)

#Parameters to check for multiple licenses and practice locations


#Parameters to ID retirees
##STRONG/MID/WEAK
##BASED ON: DEACTIVATION DATE, DEACTIVATION REASON CODE, REACTIVATION DATE, LAST UPDATE DATE, CERTIFICATION DATE, PRIMARY PRACTICE ADDRESS, ENUMERATION DATE, HEALTHCARE PROVIDER TAXONOMY

#Parameters to ID those who left the field

#Parameters to ID the start date

#Parameters to do geocoding

#Parameters to account for fellowships and residencies
##TAXONOMY CODE, INC. TRAINING-RELATED CODES THAT INCLUDE "RESIDENT/STUDENT/"FELLOW(?)"
##ORGANIZATION NAME / PRACTICE LOCATION
##ENUMERATION DATE MAY PREDATE PRACTICE START BY 3-7 YEARS

trainee_codes <- c("390200000X" #Student in an Organized Health Care Education/Training Program
                   #Others?
                   )



#####################################################################################
#### Completed Checks ###############################################################
#####################################################################################

#What is the difference between the enumeration date and certification date fields?
#Enumeration is the date that the NPI was officially signed, certification date is the last date that the provider(s) attested that the information was up-to-date and accurate. Checked if any cases exist where certification is earlier than enumeration. There are none. However, many have NAs for certification. Last Update Date is likely better as there are no NAs there.

#What does the `Is Sole Proprietor` field show us?
##Medicare/Medicaid and private payers treat sole proprietors differently, including the fact that they have no liability shield. A sole proprietor cannot have more than one NPI.
#Will restricting to only provider type == 1 miss any providers? E.g., would anyone be practicing under an organizational NPI, but not have an individual NPI? Likely yes, but a small number of patients. 
#For now, we will ignore the `Is Sole Proprietor` field, until given reason otherwise. It's only present (at least based on Chunk 1, the first million records) for Type 1 entities.

#Based on Chunk 1
#1) SOLE PROPRIETORS + TYPE 1 - 136,448
#2) SOLE PROPRIETORS + TYPE 2 - 0 
#3) NON-SOLE PROPRIETORS + TYPE 1 - 511,143
#4) NON-SOLE PROPRIETORS + TYPE 2 - 0
#5) NA PROPRIETOR + TYPE 2 - 181,602
#6) NA PROPRIETOR + NA TYPE - 81,501
#7) X PROPRIETOR + TYPE 1 - 89,306

#It's unclear what the X indicates under Sole Proprietorship

NPI_chunk_1 %>%
  group_by(
    `Entity Type Code`,
    `Is Sole Proprietor`
  ) %>%
  tally()

#What does Status tell us? Best I can tell, nothing. Doesn't actually seem to be present in the dataset.

#What does the `Is Organization Subpart` show us? Shouldn't be anything. This applies to Type 2 entities. ALl NAs for our Type 1 entities.

#What are the ones where `Entity Type Code` is NA? Seem to be ones where they've been deactivated and (all?) other fields are NAs except for the deactivation date and the NPI number itself

#What is the import of `Replacement NPI`? OSTENSIBLY (NEED TO VERIFY THIS), FLAGS DATA ENTRY ERRORS, MOVE FROM TYPE 2 TO TYPE 1, NPI ISSUED IN EARLY YEARS BEFORE FINAL CMS ENUMERATION GUIDELINES WERE IN PLACE, OR DUPLICATE NPIS ACCIDENTALLY ISSUED AND THEN CONSOLIDATED

#What is the import of the various `Provider Other... name and credential details? These are other sets of identifiers (apart from NPIs) for other health plans, payers, programs, etc.

#What is the import of the various `Authorized Official... name and associated details?`These are the authorized officials FOR TYPE 2 NPI organizational entities

#What is the import of the various `Healthcare Provider Primary Taxonomy Switch_1` type fields? Seems to be an X if this is the level (e.g., _1, _2, _3, etc.) to use. A Y if it is not.


#On checking cases where their last reported taxonomy code is not a podiatry code...

NPI_chunk_1_test %>%
  filter(Has_Podiatrist_Taxonomy == "Yes") %>%
  filter((Last_Taxonomy_Code_Num - Last_Podiatrist_Taxonomy_Num) != 0) %>% 
  group_by(Last_Taxonomy_Code) %>% 
  tally() %>% 
  arrange(desc(n))

# A fair number have moved into DME or other areas of medicine (check their current licensure/certifications)

# 9 332B00000X Durable Medical Equipment & Medical Supplies
# 4 174400000X Specialist !!! This one is interesting. Unclear what it means.
# 4 207XX0004X Orthopaedic Foot and Ankle Surgery Physician !!! This one is interesting, it's podiatrists who became orthopods
# 2 207Q00000X Family Medicine Physician
# 2 207X00000X Orthopaedic Surgery Physician !!! This one is interesting, it's podiatrists who became orthopaedic surgeons
# 2 208100000X Physical Medicine & Rehabilitation Physician
# 2 208D00000X General Practice Physician
# 14 more rows with a single entry

#####################################################################################
#### Outstanding Issues #############################################################
#####################################################################################
#anyone be practicing under an organizational NPI, but not have an individual NPI
#set for entry date. Just people entering after a certain date?
#Capturing people who WERE DPMs, but have new specialties correctly?
#How to use the `Provider Other Last Name Type Code` field?
#Seemingly, Code 1: Former Name, 2: Professional Name, 3: Other, 4: Unknown (per ChatGPT). However, I see no 4s, a large number of "5"s, and a huge number of NAs. The NAs are not concerning, the 5s and the absence of 4s are questions though.
#Cross reference with PECOS for Medicare enrollment, state licensure boards (CA, NJ, MD, FSMB), or CMS Open Payments, or Medicare utilization fiLES, or National Practitioner Data Bank (NPDB) Public Use File



#####################################################################################
#### Cleanup ########################################################################
#####################################################################################
rm(NPI_chunk_1)
