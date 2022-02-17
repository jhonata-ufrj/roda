# %%
import pandas as pd
import numpy as np
from fuzzywuzzy import process, fuzz

ramen = pd.read_excel('The-Big-List-All-reviews-up-to-3950.xlsx')

for col in ramen[['Brand','Variety','Style','Country']]:
    ramen[col] = ramen[col].str.strip()
    print('Number of unique values in ' + str(col) +': ' + str(ramen[col].nunique()))


# %%
unique_brand = ramen['Brand'].unique().tolist()
sorted(unique_brand)[:20]

unique_brand
# %%

var = '7 Select'

a = process.extract(var, unique_brand, scorer=fuzz.token_sort_ratio)
a
# %%

# %%

a = process.extract('A-Sha', unique_brand, scorer=fuzz.token_set_ratio)
a
# %%
a = process.extract('7 Select', unique_brand, scorer=fuzz.token_set_ratio)
a

# %%
#Create tuples of brand names, matched brand names, and the score
score_sort = [(x,) + i
             for x in unique_brand 
             for i in process.extract(x, unique_brand,     scorer=fuzz.token_sort_ratio)]
#Create a dataframe from the tuples
similarity_sort = pd.DataFrame(score_sort, columns=['brand_sort','match_sort','score_sort'])

similarity_sort


# %%

similarity_sort['sorted_brand_sort'] = np.minimum(similarity_sort['brand_sort'], similarity_sort['match_sort'])

similarity_sort.to_excel('similar1_2.xlsx')
# %%
high_score_sort = similarity_sort[(similarity_sort['score_sort'] >= 80) &
                (similarity_sort['brand_sort'] !=  similarity_sort['match_sort']) &
                (similarity_sort['sorted_brand_sort'] != similarity_sort['match_sort'])]
high_score_sort = high_score_sort.drop('sorted_brand_sort',axis=1).copy()
high_score_sort.to_excel('similar.xlsx')
# %%
high_score_sort.groupby(['brand_sort','score_sort']).agg(
                        {'match_sort': ', '.join}).sort_values(
                        ['score_sort'], ascending=False)
# %%
