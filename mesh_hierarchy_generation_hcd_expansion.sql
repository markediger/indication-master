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

-- TREENUM EXPANSION
WITH RECURSIVE hier_cte AS (
  SELECT
    CUI,
    HCD AS OriginalHCD,
    SPLIT_PART(HCD, '.', 1) AS HierarchyLevel,
    1 AS Depth
  FROM
    "ACORN_DSE_SANDBOX_IE"."UMLS"."MRHIER"
    WHERE SAB = 'MSH' AND (HCD like 'M01.955%' or HCD like 'C%')
  UNION ALL
  
  SELECT
    p.CUI,
    p.OriginalHCD,
    CONCAT_WS('.', p.HierarchyLevel, SPLIT_PART(SPLIT_PART(p.OriginalHCD, '.', p.Depth + 1), '.', -1)) AS HierarchyLevel,
    p.Depth + 1 AS Depth
  FROM
    hier_cte AS p
    INNER JOIN "ACORN_DSE_SANDBOX_IE"."UMLS"."MRHIER" AS c ON p.CUI = c.CUI AND p.OriginalHCD = c.HCD
  WHERE p.Depth < LENGTH(p.OriginalHCD) - LENGTH(REPLACE(p.OriginalHCD, '.', ''))+1
)
SELECT DISTINCT 
  hier_cte.CUI as ParentCUI,
  hier_cte.HierarchyLevel AS Parent_TreeNum,
  hier_cte.OriginalHCD,
  hier_cte.Depth,
  lookup1.CODE as ParentDUI,
  lookup1.STR as ParentTerm
FROM
  hier_cte
LEFT JOIN cui_hcd_lookup1 AS lookup1 ON hier_cte.CUI = lookup1.CUI
ORDER BY
  hier_cte.CUI,
  hier_cte.OriginalHCD,
  hier_cte.Depth;