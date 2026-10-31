-- One clean row per entity in the current release.
-- 'na' is ACRA's null. Dates, postcodes and industry codes are typed and validated,
-- and the status labels are grouped (macros.sql).
CREATE OR REPLACE TABLE stg_entities AS
WITH src AS (
    SELECT
        trim(uen)                                                    AS uen,
        nullif(trim(entity_name), 'na')                              AS entity_name,
        trim(entity_type_description)                                AS entity_type,
        nullif(trim(business_constitution_description), 'na')        AS business_constitution,
        nullif(trim(company_type_description), 'na')                 AS company_type,
        trim(entity_status_description)                              AS status,
        try_cast(registration_incorporation_date AS DATE)            AS registration_date,
        nullif(trim(postal_code), 'na')                              AS postal_code,
        nullif(trim(primary_ssic_code), 'na')                        AS ssic_code_raw,
        nullif(trim(primary_ssic_description), 'na')                 AS ssic_description,
        snapshot_date
    FROM raw_acra
)
SELECT
    s.uen,
    s.entity_name,
    s.entity_type,
    CASE
        WHEN s.entity_type = 'Sole Proprietorship/ Partnership' THEN 'Sole proprietor or partnership'
        WHEN s.entity_type = 'Local Company'                    THEN 'Company'
        WHEN s.entity_type = 'Limited Liability Partnership'    THEN 'LLP'
        WHEN s.entity_type = 'Limited Partnership'              THEN 'LP'
        WHEN s.entity_type = 'Foreign Company Branch'           THEN 'Foreign branch'
        ELSE 'Other'
    END                                                          AS entity_kind,
    s.business_constitution,
    s.company_type,
    s.status,
    status_group(s.status)                                       AS status_group,
    s.registration_date,
    year(s.registration_date)                                    AS registration_year,
    s.postal_code,
    valid_postcode(s.postal_code)                                AS postal_code_valid,
    CASE WHEN valid_postcode(s.postal_code) THEN left(s.postal_code, 2) END AS postal_sector,
    s.ssic_code_raw,
    CASE WHEN valid_ssic(s.ssic_code_raw) THEN s.ssic_code_raw END         AS ssic_code,
    CASE WHEN valid_ssic(s.ssic_code_raw) THEN left(s.ssic_code_raw, 2) END AS ssic_division,
    s.ssic_description,
    sec.section                                                  AS ssic_section,
    coalesce(sec.section_name, 'Unclassified')                   AS industry,
    coalesce(valid_ssic(s.ssic_code_raw) AND left(s.ssic_code_raw, 2) = '56', FALSE) AS is_food_and_beverage,
    s.snapshot_date
FROM src s
LEFT JOIN ssic_sections sec
       ON valid_ssic(s.ssic_code_raw)
      AND left(s.ssic_code_raw, 2) BETWEEN sec.division_from AND sec.division_to;
