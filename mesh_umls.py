import os
import pandas as pd
import snowflake.connector

def expand_tree_numbers(tree_number):
    parts = tree_number.split('.')
    parent_trees = ['.'.join(parts[:i+1]) for i in range(len(parts))]
    return list(dict.fromkeys(parent_trees))

# Query for MeSH Main Headers / Descriptor-level data
base_descriptor_terms_query = """
SELECT DISTINCT mrc.CODE as DUI, mrc.STR as TERM, mrh.HCD as TREENUM
FROM ACORN_DSE_SANDBOX_IE.UMLS.MRHIER mrh
LEFT JOIN ACORN_DSE_SANDBOX_IE.UMLS.MRCONSO mrc
ON mrh.CUI = mrc.CUI
WHERE mrc.SAB = 'MSH' AND mrh.SAB = 'MSH' AND mrc.TTY = 'MH' AND (mrh.HCD like 'M01.955%' or mrh.HCD like 'C%' or mrh.HCD like 'F03%')
"""

base_descriptor_terms = pd.read_sql_query(base_descriptor_terms_query, con)

# Query for MeSH Supplemental Terms
base_supplemental_terms_query = """
SELECT DISTINCT mrc.CODE as CHILDDUI, mrc2.CODE as ParentDUI, mrc.TTY, mrc.STR as SUPPTERM
FROM ACORN_DSE_SANDBOX_IE.UMLS.MRREL rel
LEFT JOIN ACORN_DSE_SANDBOX_IE.UMLS.MRCONSO mrc ON rel.CUI1 = mrc.CUI
LEFT JOIN ACORN_DSE_SANDBOX_IE.UMLS.MRCONSO mrc2 ON rel.CUI2 = mrc2.CUI
WHERE mrc.SAB = 'MSH' AND rel.RELA = 'mapped_from' AND mrc.TTY = 'NM' AND mrc2.SAB = 'MSH' AND rel.CUI2 in
(select distinct CUI from ACORN_DSE_SANDBOX_IE.UMLS.MRHIER m where SAB = 'MSH' and (HCD like 'M01.955%' or HCD like 'C%' or HCD like 'F03%'))
"""

base_supplemental_terms = pd.read_sql_query(base_supplemental_terms_query, con)

base_custom_terms = pd.read_csv('mesh_extensions.csv')

base_custom_terms = base_custom_terms[['CHILDDUI', 'PARENTDUI', 'SUPPTERM']]
base_supp_terms['TERMTYPE'] = 'SUPP'
base_custom_terms['TERMTYPE'] = 'MEDIDATA'

# Bind base supplemental and custom Medidata terms
all_supp_terms = pd.concat([base_supp_terms, base_custom_terms])

# Inner join descriptor and supplemental terms on each supple
supp_df = pd.merge(base_descriptor_terms, all_supp_terms, left_on='DUI', right_on='PARENTDUI', how='inner')

# Generate TREENUM_SUPP by concatenating TREENUM with a sequential number
supp_df['TREENUM_SUPP'] = supp_df.groupby(['DUI', 'TERM', 'TREENUM']).cumcount() + 1
supp_df['TREENUM_SUPP'] = supp_df.apply(lambda x: f"{x['TREENUM']}.{x['TREENUM_SUPP']}", axis=1)

# Organize columns
supp_df = supp_df[['CHILDDUI', 'SUPPTERM', 'TREENUM_SUPP', 'TERMTYPE']]
supp_df.rename(columns={'CHILDDUI': 'DUI', 'SUPPTERM': 'TERM', 'TREENUM_SUPP': 'TREENUM'}, inplace=True)

# Add TERMTYPE = 'DESC' to base_descriptor_terms and concatenate with the merged data frame
base_descriptor_terms['TERMTYPE'] = 'DESC'
base_hierarchy = pd.concat([supp_df, base_descriptor_terms[['DUI', 'TERM', 'TREENUM', 'TERMTYPE']]])
base_hierarchy['TREENUM'] = base_hierarchy['TREENUM'].astype(str)

# Expand tree numbers to all parents to generate hierarchy
base_hierarchy['PARENT_TREENUM'] = base_hierarchy['TREENUM'].apply(expand_tree_numbers)
base_hierarchy_exploded = base_hierarchy.explode('PARENT_TREENUM')
base_hierarchy_exploded.rename(columns={'TREENUM': 'TARGET_TREENUM', 'DUI': 'TARGET_DUI'}, inplace=True)

# Generated cleaned version of descriptor-level terms prior to joining
base_descriptor_terms.rename(columns={'TERM': 'PARENT_TERM', 'DUI': 'PARENT_DUI'}, inplace=True)
base_descriptor_terms.drop('TERMTYPE', axis=1, inplace=True)

# Join descriptor-level terms back to exploded hierarchy and clean final hierarchy
final_hierarchy = pd.merge(base_hierarchy_exploded, base_descriptor_terms, left_on='PARENT_TREENUM', right_on='TREENUM', how='left')
final_hierarchy = final_hierarchy[final_hierarchy['TARGET_TREENUM'] != final_hierarchy['PARENT_TREENUM']]
final_hierarchy.drop('TREENUM', axis=1, inplace=True)
