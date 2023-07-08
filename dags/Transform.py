from Extract import *

## Transform Data
def cleansing():
    result_data = buat_list()
    df = pd.DataFrame(result_data)

    Country_id = []
    for x in range(hitung_banyak_dict_dalam_json()-1):
        Country_id.append(x)

    df['Country_ID'] = Country_id
    df['New Cases_text'] = df['New Cases_text'].str.replace('+', '')
    df['New Cases_text'] = df['New Cases_text'].str.replace('N/A', '0')
    df['New Cases_text'] = df['New Cases_text'].replace('', 0)
   

    df['New Deaths_text'] = df['New Deaths_text'].str.replace('+', '')
    df['New Deaths_text'] = df['New Deaths_text'].str.replace('N/A', '0')
    df['New Deaths_text'] = df['New Deaths_text'].replace('', 0)

    df['Total Cases_text'] = df['Total Cases_text'].replace([''], 0)
    df['Total Deaths_text'] = df['Total Deaths_text'].replace([''], 0)

    df['Total Recovered_text'] = df['Total Recovered_text'].replace([''], 0)
    df['Total Recovered_text'] = df['Total Recovered_text'].replace(['N/A'], 0)

    df['Active Cases_text'] = df['Active Cases_text'].str.replace('+', '')
    df['Active Cases_text'] = df['Active Cases_text'].str.replace('N/A', '0')
    df['Active Cases_text'] = df['Active Cases_text'].replace('', 0)
    df = df.replace(',','', regex=True)

    #mengubah type data 
    df['Last Update'] = pd.to_datetime(df['Last Update'])
    df = df.to_json()
    return df


# def cleansing_data():
#     df = pd.read_json(cleansing())

#     # Cleansing data yang kosong atau NA diubang menjadi 0 
#     #df['New Cases_text'] = df['New Cases_text'].replace([''], 0)
#     df['New Cases_text'] = df['New Cases_text'].str.replace('+', '')
#     df['New Cases_text'] = df['New Cases_text'].str.replace('N/A', '0')
#     df['New Cases_text'] = df['New Cases_text'].replace('', 0)
   

#     df['New Deaths_text'] = df['New Deaths_text'].str.replace('+', '')
#     df['New Deaths_text'] = df['New Deaths_text'].str.replace('N/A', '0')
#     df['New Deaths_text'] = df['New Deaths_text'].replace('', 0)

#     df['Total Cases_text'] = df['Total Cases_text'].replace([''], 0)
#     df['Total Deaths_text'] = df['Total Deaths_text'].replace([''], 0)

#     df['Total Recovered_text'] = df['Total Recovered_text'].replace([''], 0)
#     df['Total Recovered_text'] = df['Total Recovered_text'].replace(['N/A'], 0)

#     df['Active Cases_text'] = df['Active Cases_text'].str.replace('+', '')
#     df['Active Cases_text'] = df['Active Cases_text'].str.replace('N/A', '0')
#     df['Active Cases_text'] = df['Active Cases_text'].replace('', 0)
#     df = df.replace(',','', regex=True)

#     #mengubah type data 
#     df['Last Update'] = pd.to_datetime(df['Last Update'])
#     # df = df.astype
#     # ({
#     #     'Active Cases_text' : 'int',
#     #     'New Cases_text' : 'int', 
#     #     'New Deaths_text' : 'int', 
#     #     'Total Cases_text' : 'int64', 
#     #     'Total Deaths_text' : 'int', 
#     #     'Total Recovered_text':'int'
#     # })
    
#     return df


# def createtable_Country():
#     #df = cleansing_data()
#     df = pd.read_json(cleansing())
#     df_Country = df[['Country_ID', 'Country_text']]
    
#     return df_Country


# def createtable_ActiveCasesData():
#     df = pd.read_json(cleansing())
#     df_Active_Cases = df[(df["Active Cases_text"]!=0) & (df['Active Cases_text'] != 'N/A')].reset_index()
#     df_Active_Cases = df_Active_Cases[['Country_ID', 'Active Cases_text','Total Cases_text','Total Recovered_text', 'Last Update']]
   
#     return df_Active_Cases


# def createtable_NewCaseData():
#     df = pd.read_json(cleansing())
#     df_cases = df[(df['New Cases_text'] != 0 ) & (df['New Cases_text'] != 'N/A')].reset_index()
#     df_cases = df_cases[['Country_ID', 'New Cases_text','Last Update']]

#     return df_cases

# def createtable_DeathsData():
#     df = pd.read_json(cleansing())
#     df_Death = df[(df['New Deaths_text'] != 0 ) & (df['New Deaths_text'] != 'N/A')].reset_index()
#     df_Death = df_Death[['Country_ID', 'New Deaths_text', 'Total Deaths_text', 'Last Update']]

#     return df_Death 


#Load data to database