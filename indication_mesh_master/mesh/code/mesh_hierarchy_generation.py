import os
import pandas as pd

import util.common_funcs as cf


# Load MeSH Main Headers / Descriptor-level data
base_descriptor_terms_query = """
SELECT DISTINCT mrc.CODE as DUI, mrc.STR as TERM, mrh.HCD as TREENUM
FROM ACORN_DSE_PROD_MDR.UMLS.MRHIER mrh
LEFT JOIN ACORN_DSE_PROD_MDR.UMLS.MRCONSO mrc
ON mrh.CUI = mrc.CUI
WHERE mrc.SAB = 'MSH' AND mrh.SAB = 'MSH' AND mrc.TTY = 'MH'
"""

base_descriptor_terms = cf.snowflakeExecuteQuery(base_descriptor_terms_query)

# Load MeSH Supplemental Terms
base_supplemental_terms_query = """
SELECT DISTINCT mrc.CODE as CHILDDUI, mrc2.CODE as ParentDUI, mrc.TTY, mrc.STR as SUPPTERM
FROM ACORN_DSE_PROD_MDR.UMLS.MRREL rel
LEFT JOIN ACORN_DSE_PROD_MDR.UMLS.MRCONSO mrc ON rel.CUI1 = mrc.CUI
LEFT JOIN ACORN_DSE_PROD_MDR.UMLS.MRCONSO mrc2 ON rel.CUI2 = mrc2.CUI
WHERE mrc.SAB = 'MSH' AND rel.RELA = 'mapped_from' AND mrc.TTY = 'NM' AND mrc2.SAB = 'MSH' AND rel.CUI2 in
(select distinct CUI from ACORN_DSE_PROD_MDR.UMLS.MRHIER m where SAB = 'MSH' and (HCD like 'M01.955%' or HCD like 'C%'))
"""
base_supplemental_terms = cf.snowflakeExecuteQuery(base_supplemental_terms_query)

# Assemble MeSH Hierarchy
# Inner join descriptor and supplemental terms on each supplemental
supp_df = pd.merge(base_descriptor_terms, base_supplemental_terms, left_on='DUI', right_on='PARENTDUI', how='inner')

# Generate TREENUM_SUPP by concatenating TREENUM with a sequential number
supp_df['TREENUM_SUPP'] = supp_df.groupby(['DUI', 'TERM', 'TREENUM']).cumcount() + 1
supp_df['TREENUM_SUPP'] = supp_df.apply(lambda x: f"{x['TREENUM']}.{x['TREENUM_SUPP']}", axis=1)

# Add TERMTYPE to identify type once joined
supp_df['TERMTYPE'] = 'SUPP'

# Organize columns
supp_df = supp_df[['CHILDDUI', 'SUPPTERM', 'TREENUM_SUPP', 'TERMTYPE']]
supp_df.rename(columns={'CHILDDUI': 'DUI', 'SUPPTERM': 'TERM', 'TREENUM_SUPP': 'TREENUM'}, inplace=True)

# Add TERMTYPE = 'DESC' to base_descriptor_terms and concatenate with the merged data frame
base_descriptor_terms['TERMTYPE'] = 'DESC'
final_hierarchy = pd.concat([supp_df, base_descriptor_terms[['DUI', 'TERM', 'TREENUM', 'TERMTYPE']]])
final_hierarchy['PARENT_TREENUM'] = final_hierarchy['TREENUM'].apply(cf.expand_tree_numbers)

final_hierarchy_exploded = final_hierarchy.explode('PARENT_TREENUM')

# Sort
final_hierarchy.sort_values(by=['DUI', 'TREENUM'], inplace=True)