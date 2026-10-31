-- Business rules shared by staging and marts, defined once.

-- ACRA has 37 status labels. Grouped into the four states a reader needs.
-- Order matters: a receiver-appointed company is "Live (...)" but in distress.
CREATE OR REPLACE MACRO status_group(s) AS
    CASE
        WHEN s IS NULL                                               THEN NULL
        WHEN s LIKE 'Live (Receiver%'                                THEN 'closing'
        WHEN s IN ('Live', 'Live Company')                           THEN 'live'
        WHEN s LIKE 'In Liquidation%'
          OR s LIKE 'Gazetted To Be Struck Off%'
          OR s LIKE '%In Progress'
          OR s LIKE 'Under Judicial Management%'
          OR s IN ('To Be Ceased', 'Suspended')                      THEN 'closing'
        -- Converted, merged or re-domiciled: the business carries on as another entity.
        WHEN s LIKE 'Converted%' OR s = 'Amalgamated'
          OR s = 'Ceased Registration (Transferred to Singapore)'    THEN 'converted'
        WHEN s LIKE 'Struck Off%'
          OR s LIKE 'Cancelled%'
          OR s LIKE 'Dissolved%'
          OR s LIKE 'Removed%'
          OR s LIKE 'Revoked%'
          OR s LIKE 'Registration expired%'
          OR s IN ('Terminated', 'Ceased Registration', 'Expired')   THEN 'closed'
        ELSE 'unknown'
    END;

-- Singapore moved to 6-digit postcodes in 1995. 4-digit codes are pre-1995 and cannot
-- be placed on a map, so they are flagged rather than padded. Sectors (the first two
-- digits) run 01 to 82.
CREATE OR REPLACE MACRO valid_postcode(p) AS
    regexp_full_match(coalesce(p, ''), '\d{6}') AND left(p, 2) BETWEEN '01' AND '82';

-- SSIC 2020 codes are 5 digits. 4-digit codes are a legacy scheme with no description.
CREATE OR REPLACE MACRO valid_ssic(c) AS regexp_full_match(coalesce(c, ''), '\d{5}');
