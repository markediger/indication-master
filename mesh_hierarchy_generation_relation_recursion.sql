CREATE TEMPORARY TABLE IF NOT EXISTS cui_hcd_lookup1 AS (
(WITH RankedResults AS (
    SELECT 
        mrh.CUI, 
        mrc.CODE, 
        mrc.STR, 
        mrc.TTY,
        mrh.HCD as TreeNums,
        ROW_NUMBER() OVER (PARTITION BY mrc.CODE ORDER BY 
            CASE mrc.TTY
                WHEN 'MH' THEN 1
                WHEN 'PEP' THEN 2
                WHEN 'ET' THEN 3
                WHEN 'NM' THEN 4
                ELSE 99
            END) as rn
    FROM "ACORN_DSE_SANDBOX_IE"."UMLS"."MRHIER" mrh 
    LEFT JOIN "ACORN_DSE_SANDBOX_IE"."UMLS"."MRCONSO" mrc ON mrh.CUI = mrc.CUI
    WHERE mrc.SAB = 'MSH' AND mrh.SAB = 'MSH' AND (mrh.HCD like 'M01.955%' or mrh.HCD like 'C%')
)
SELECT 
    CUI, 
    CODE, 
    STR, 
    TreeNums
FROM RankedResults
WHERE rn = 1
ORDER BY CODE)

        UNION ALL

(SELECT
    CUI1 as CUI,
    CODE,
    STR,
    CONCAT(HCD, '.', LPAD(CAST(ROW_NUMBER() OVER (PARTITION BY HCD ORDER BY CUI1, CUI2, STR) AS CHAR(3)), 3, '0')) AS TreeNums
FROM
    (SELECT distinct rel.CUI1, rel.CUI2, mrc.CODE, mrc.STR, hier.HCD 
FROM "ACORN_DSE_SANDBOX_IE"."UMLS"."MRREL" rel
LEFT JOIN "ACORN_DSE_SANDBOX_IE"."UMLS"."MRCONSO" mrc ON rel.CUI1 = mrc.CUI 
LEFT JOIN "ACORN_DSE_SANDBOX_IE"."UMLS"."MRHIER" hier ON rel.CUI2 = hier.CUI
where mrc.SAB = 'MSH' and rel.RELA = 'mapped_from' and mrc.TTY = 'NM' and hier.SAB = 'MSH' and (hier.HCD like 'M01.955%' or hier.HCD like 'C%')) AS ordered_table))

CREATE TEMPORARY TABLE IF NOT EXISTS cui_hcd_lookup2 AS SELECT * from cui_hcd_lookup1

WITH RECURSIVE mesh AS (
    -- Base case: Select top-level descriptors (those without a parent in MeSH)
    SELECT 
        m.CUI1 AS "CHILDTERMID",  -- In 'PAR', CUI1 is the child
        m.CUI2 AS "PARENTTERMID",  -- In 'PAR', CUI2 is the parent
        1 AS "LEVEL"  -- Starting level for top-level descriptors
    FROM 
        "ACORN_DSE_SANDBOX_IE"."UMLS"."MRREL" AS m
    WHERE 
        m.SAB = 'MSH' AND
        m.REL = 'PAR' AND
        m.CUI1 = 'C0012674' -- Choose starting level for hierarchy
    UNION ALL
    -- Recursive part: Find children for each term identified in the previous step
    SELECT 
        m.CUI1,  -- Child in the current relationship
        mh.CHILDTERMID AS "PARENTTERMID",  -- Parent from the previous level of recursion
        mh.LEVEL + 1 AS "LEVEL"
    FROM 
        mesh AS mh
    JOIN 
        "ACORN_DSE_SANDBOX_IE"."UMLS"."MRREL" AS m ON mh.CHILDTERMID = m.CUI2  -- Join on the parent term from the previous level to find its children
    WHERE 
        m.SAB = 'MSH' AND
        m.REL in ('PAR', 'RB')
)
SELECT DISTINCT mesh.PARENTTERMID, mesh.CHILDTERMID, mesh.LEVEL, lookup1.CODE AS ChildDUI, lookup1.STR AS ChildTerm, 
lookup1.TreeNums AS ChildTreeNums, lookup2.CODE AS ParentDUI, lookup2.STR AS ParentTerm, lookup2.TreeNums AS ParentTreeNums
FROM mesh
LEFT JOIN cui_hcd_lookup1 AS lookup1 ON mesh.ChildTermID = lookup1.CUI
LEFT JOIN cui_hcd_lookup2 AS lookup2 ON mesh.ParentTermID = lookup2.CUI
ORDER BY PARENTTERMID, CHILDTERMID, LEVEL;