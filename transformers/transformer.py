if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import gspread
import pandas as pd
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe

scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


#Directly read from google sheets

creds = ServiceAccountCredentials.from_json_keyfile_name(filename='./secret_key.json', scopes=scope)
file_object = gspread.authorize(creds)

df_names = ['2008-2012', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023'] # SCOPE OF YEARS


def read_tables(df_names):
    tables_ = {}    
    dataframes = {}     
    for name in df_names:
        if name == '2008-2012':
            tables_[name] = file_object.open('2008-2012_femicide_data')

            sheets_2008_2012 = tables_[name].worksheets()
            for sheet in sheets_2008_2012:
                data = sheet.get_all_records()   
                df = pd.DataFrame(data)          
                dataframes[sheet.title] = df    
        else:
            tables_[name] = file_object.open(f'{name}_femicide_data')
            dataframes[name] = get_as_dataframe(tables_[name].sheet1)
    return tables_ , dataframes


femicides , dfs = read_tables(df_names)


###################################################### COLUMN CLEANING ######################################################

patterns = [
    '^Unnamed',
    '^Ay Sayilari',
    '^Not',
    '^=If',
    '^Kadının Çalışma Durumu_yeni',
    '^Tekrar Eden Data\n',
    'Aralığı',
    '^Working Status of the Woman',
    '^ ',
    '^Veri Girişini Yapan Kişi\nİsim Soyisim',
    'Baş Harfler',
    '6284 Tedbirleri',
    'link',
    'Bölge',
    'KARARSIZ KALINAN HABER',
    'ÖNEMLİ GÖRÜLEN HABER'
    ]

names = ['2008', '2009', '2010', '2011', '2012', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023']

def clean_dfs(patterns, names):
    updated_dfs = {}    
    for name in names:
        patterns_lower = [pattern.lower() for pattern in patterns]
        for pattern in patterns_lower:
            dfs[name] = dfs[name].loc[:, ~dfs[name].columns.str.lower().str.contains(pattern)]
        updated_dfs[name] = dfs[name]     
    return updated_dfs


femicide_df = clean_dfs(patterns, names)


###################################################### RENAME COLUMNS ######################################################

rename_rules = {
    'Kadının Adı' : 'women_name' , 
    'Kadının İsmi' : 'women_name' , 
    'AD SOYAD' : 'women_name' ,
    'YIL' : 'partition_year' ,
    'HABER KAYNAĞI' : 'informant_of_data',
    'Haber Linki' : 'informant_of_data',
    'Link' : 'informant_of_data',
    'Tarih' : 'historical_date' , 
    'Ay' : 'month_of_femicide',
    'İl' : 'province_of_femicide' , 
    'Nerede' : 'place_of_femicide' ,
    'Nerede?' : 'place_of_femicide' ,
    'Kadının Yaşı' : 'women_age',
    'Kadının Çalışma Durumu' : 'women_employement_status' ,
    'Engel Durumu' : 'women_disability_status' ,
    'Katil / Şüphelinin Yakınlığı' : 'by_whom' , 
    'Katilin / Şüphelinin Yakınlığı' : 'by_whom' ,
    'Katil / Şüphelinin Adı' : 'murderer_name' , 
    'Katil / Şüpheli Adı' : 'murderer_name' ,
    'Katilin Yaşı' : 'murderer_age' ,
    'Katil / Şüphelinin Yaşı' : 'murderer_age' ,
    'Silah' : 'weapon' ,
    'Bahane' : 'pretext' ,
    'Çocuk Sayısı' : 'number_of_children',
    'Çocuk Sayı' : 'number_of_children',
    'Çocuk_var_yok' : 'child_info' ,
    'Çocuk Bilgisi' : 'child_info' 
}

for key, dfs in femicide_df.items():
    femicide_df[key] = dfs.rename(columns=rename_rules)


###################################################### DETECT INTERSECTION COLUMNS ######################################################

# Start with 2008
common_columns = set(femicide_df['2008'].columns) 

for key, dfs in femicide_df.items():
    if key != '2008':
        common_columns = common_columns.intersection(set(dfs.columns))



########################################>>>>>>>>>>>>>> FIXED RAW DATA ISSUES <<<<<<<<<<<<<<########################################

# 2017 Data Unique Fix
femicide_df['2017'][femicide_df['2017']['women_name'].isnull()]
femicide_df['2017'].loc[132 , ['women_name']] = '*'
femicide_df['2017'].loc[275 , ['women_name']] = '*'
femicide_df['2017'].loc[281 , ['women_name']] = '*'


femicide_data_frame = femicide_df['2008']

for key, dfs in femicide_df.items():     
    if key != '2008':
        femicide_data_frame = pd.concat([femicide_data_frame, femicide_df[key]], ignore_index=True)

femicide_data_frame.dropna(subset=['women_name'] , inplace=True , ignore_index=True)

mask = femicide_data_frame['historical_date'].isnull()
femicide_data_frame.loc[mask, 'historical_date'] = femicide_data_frame.loc[mask, 'partition_year']
femicide_data_frame.filter(items=['historical_date', "partition_year"])

femicide_data_frame.loc[femicide_data_frame['historical_date'] == '#########', 'historical_date'] = '2016'
femicide_data_frame.loc[femicide_data_frame['historical_date'] == '########', 'historical_date'] = '2016'
femicide_data_frame.loc[1471 , ['historical_date']] = '2017' 
femicide_data_frame.loc[768 , ['historical_date' , "partition_year"]] = '2012'
femicide_data_frame.loc[767 , ['historical_date' , "partition_year"]] = '2015'
femicide_data_frame.loc[630 , ['historical_date' , "partition_year"]] = '2012'
femicide_data_frame.loc[1470 , ['historical_date' , "partition_year"]] = '2017'
femicide_data_frame.loc[1472 , ['historical_date' , "partition_year"]] = '2017'
femicide_data_frame.loc[2164 , ['historical_date']] = '05.03.2019'
femicide_data_frame.loc[2165 , ['historical_date']] = '05.03.2019'
femicide_data_frame.loc[2164 , ["partition_year"]] = '2019'
femicide_data_frame.loc[2165 , ["partition_year"]] = '2019'
femicide_data_frame.loc[444 , ['historical_date' , "partition_year"]] = '2011'
femicide_data_frame.loc[519 , ['historical_date' , "partition_year"]] = '2012'
femicide_data_frame.loc[579 , ['historical_date' , "partition_year"]] = '2012'
femicide_data_frame.loc[3729 , ["partition_year"]] = '2023'
femicide_data_frame.loc[3729 , ['historical_date']] = '02.11.2023'
femicide_data_frame.loc[1053:1065 , 'historical_date'] = '2016'


pyear_is_null = femicide_data_frame['partition_year'].isnull()
femicide_data_frame.loc[pyear_is_null, 'partition_year'] = femicide_data_frame.loc[pyear_is_null, 'historical_date'].str[-4:]

pyear_mask = femicide_data_frame['partition_year'].str.len() > 4
femicide_data_frame.loc[pyear_mask , 'partition_year'] = femicide_data_frame.loc[pyear_mask, 'partition_year'].str[-4:]

femicide_data_frame['partition_year'] = femicide_data_frame['partition_year'].astype(str) 
femicide_data_frame["historical_date"] = femicide_data_frame["historical_date"].apply(lambda x: x.replace('/', '.') if isinstance(x, str) and '/' in x else x)

unknown_rules = ["*", "Tespit Edilemeyen", "Bilinmiyor", "?", "Belirtilmemiş" , "tespit edilemeyen" , "belirtilmemiş", "-", "BELİRTİLMEMİŞ", "bilinmiyor"]

femicide_data_frame.replace(unknown_rules, "unknown", inplace=True) 
mask_2 = femicide_data_frame["women_age"].str.contains("-").fillna(False)
femicide_data_frame.loc[mask_2 , "women_age"] = "unknown"
femicide_data_frame[mask_2]


age_is_null = femicide_data_frame["women_age"].isnull()
femicide_data_frame.loc[age_is_null, ["women_age"]] = "unknown"
mask_3 = femicide_data_frame['women_age'].str.endswith('aylık').fillna(False) 
femicide_data_frame.loc[mask_3 , ["women_age"]] = "0"

femicide_data_frame.loc[1799 , ["women_age"]] = "unknown"

femicide_data_frame['women_age'] = pd.to_numeric(femicide_data_frame["women_age"], errors="ignore")

is_numeric = pd.to_numeric(femicide_data_frame['women_age'], errors='coerce')


conditions_of_ages = [
    is_numeric < 18, 
    (is_numeric >= 18) & (is_numeric < 30) ,
    (is_numeric >= 30) & (is_numeric < 50),
    (is_numeric >= 50) & (is_numeric < 65),
    is_numeric >= 65
    ]

values_of_ages = [
    "children",
    "young",
    "middle_aged",
    "mature",
    "seniors"
]

femicide_data_frame["age_range"] = np.select(conditions_of_ages, values_of_ages, default="unknown")


conditions_values = {
    "yok" : "0",
    "tespit edilemeyen" : "unknown",
    "ar": "1",
    "1+": "1",
    "2+": "2",
    "3+": "3",
    "az": "1",
    "amile" : "pregnant"
}

for pattern, value in conditions_values.items():
    mask_child_no = femicide_data_frame['number_of_children'].str.contains(pattern).fillna(False)
    femicide_data_frame.loc[mask_child_no, 'number_of_children'] = value

child_is_numeric = pd.to_numeric(femicide_data_frame['number_of_children'], errors='coerce')

conditions_of_child = [
    child_is_numeric > 0, 
    child_is_numeric == 0
    ]

values_of_child = [
    "yes",
    "no"
]

femicide_data_frame["number_of_children"] = np.select(conditions_of_child, values_of_child, default="unknown")

replace_rules = {
    r"Var": "yes",
    r"Yok": "no",
    r"Hamile": "pregnant"
}


femicide_data_frame['child_info'] = femicide_data_frame['child_info'].replace(replace_rules, regex=True)

child_is_null = femicide_data_frame['child_info'].isnull()
femicide_data_frame.loc[child_is_null, 'child_info'] = femicide_data_frame.loc[child_is_null, 'number_of_children']

femicide_data_frame = femicide_data_frame.loc[:, ~femicide_data_frame.columns.str.contains('number_of_children')]
femicide_data_frame["month_of_femicide"] = femicide_data_frame["month_of_femicide"].astype(str).str.lower().str.strip()

mask_4 = femicide_data_frame['month_of_femicide'].str.endswith(' ').fillna(False) 

femicide_data_frame.loc[mask_4, 'month_of_femicide'] = femicide_data_frame.loc[mask_4, 'month_of_femicide'].str.rstrip()

replace_rules = {
    "ağustos": "august",
    "ocak": "january",
    "mayıs": "may",
    "aralık": "december",
    "mart": "march",
    "hazi̇rankocaeli̇": "june",
    "hazi̇ran": "june",
    "haziran": "june",
    "eylül": "september",
    "ekim": "october",
    "nisan": "april",
    "şubat": "february",
    "temmuz": "july",
    "nan": "unknown",
    "kasım": "november",

}


femicide_data_frame['month_of_femicide'] = femicide_data_frame['month_of_femicide'].replace(replace_rules, regex=False)
femicide_data_frame["province_of_femicide"] = femicide_data_frame["province_of_femicide"].astype(str).str.lower().str.strip()
femicide_data_frame['province_of_femicide'] = femicide_data_frame['province_of_femicide'].str.split('/').str[0].str.strip()

corrections = {
    'i̇stanbul': 'istanbul',
    'i̇zmir': 'izmir',
    'i̇zmi̇r': 'izmir',
    'kocaeli̇': 'kocaeli',
    'i̇zmit': 'kocaeli',
    'i̇zmi̇t': 'kocaeli',
    'mersi̇n': 'mersin',
    'mani̇sa': 'manisa',
    'gazi̇antep': 'gaziantep',
    'antep' : 'gaziantep',
    'mugla': 'muğla',
    'di̇yarbakir': 'diyarbakır',
    'şanli urfa': 'şanlıurfa',
    'şanlı urfa': 'şanlıurfa',
    'şanliurfa': 'şanlıurfa',
    'urfa' : 'şanlıurfa',
    'deni̇zli̇': 'denizli',
    'eski̇şehi̇r': 'eskişehir',
    'si̇vas': 'sivas',
    'eski̇şehir': 'eskişehir',
    'teki̇rdağ': 'tekirdağ',
    'afyonkarahi̇sar': 'afyonkarahisar',
    'afyon' : 'afyonkarahisar',
    'aydin': 'aydın',
    'bodrum': 'muğla',
    'milas': 'muğla',
    'balikesi̇r': 'balıkesir',
    'kirikkale': 'kırıkkale',
    'kırkkale' : 'kırıkkale',
    'si̇i̇rt': 'siirt',
    'kırklreli': 'kırklareli',
    'giresun': 'giresun',
    'gi̇resun': 'giresun',
    'kayseri̇': 'kayseri',
    'hakkâri': 'hakkari',
    'hakkari': 'hakkari',
    'şırnak': 'şırnak',
    'nevşehir': 'nevşehir',
    'çorlu': 'tekirdağ',
    'maraş' : 'kahramanmaraş',
    "nan" : "unknown"
}

femicide_data_frame['province_of_femicide'] = femicide_data_frame['province_of_femicide'].replace(corrections, regex=False)

femicide_data_frame["by_whom"] = femicide_data_frame["by_whom"].astype(str).str.lower().str.strip()

corrections = {
    # Father related
    'baba': 'father',
    'babası': 'father',
    'şüpheli, baba-abi': 'father',
    'üvey baba': 'father',
    'üvey babası': 'father',
    
    # Brother related
    'kardeş': 'brother',
    'kardeşi': 'brother',
    'kardeşi̇': 'brother',
    '2 abisi': 'brother',
    'erkek kardeş': 'brother',
    'abisi': 'brother',
    'kardeşi ve tanıdık': 'brother',
    'erkek kardeşi': 'brother',
    'ağabeyi': 'brother',
    
    # Relative related
    'akraba': 'relative',
    'akrabası': 'relative',
    'damadı': 'relative',
    'enişte': 'relative',
    'eşinin ailesi': 'relative',
    'kocasının kardeşi': 'relative',
    'kayinbi̇rader': 'relative',
    'damadi': 'relative',
    'kuzen': 'relative',
    'tanıdığı biri/akraba': 'relative',
    'tanıdığı biri/akraba (damadı)': 'relative',
    'tanıdığı biri/akraba (müşterisi)': 'relative',
    'tanıdığı biri / akraba': 'relative',
    'aile': 'relative',
    'ailesi': 'relative',
    'oğlu ve akraba': 'relative',
    'kizinin kocasi': 'relative',
    'kocasının kardeşi': 'relative',
    'kızının eski eşi': 'relative',
    'kocasinin akrabasi': 'relative',
    'eşinin kardeşi': 'relative',
    'eşi̇ni̇n kardeşi̇': 'relative',
    'eşinin erkek kardeşi': 'relative',
    'damat': 'relative',
    'ablasının kocası': 'relative',
    'ablasinin kocasi': 'relative',
    'amcası': 'relative',
    'torunu': 'relative',
    'torun': 'relative',
    'yeğeni': 'relative',
    'kayınbiraderi': 'relative',
    'kocasının yeğeni': 'relative',
    'eniştesi': 'relative',
    
    # Acquaintance related
    'tanıdık': 'acquaintance',
    'tanıdığı biri': 'acquaintance',
    'tanıdğı biri': 'acquaintance',
    'tanidiği bi̇ri̇': 'acquaintance',
    'komşusu': 'acquaintance',
    'arkadaşı': 'acquaintance',
    'müşteri': 'acquaintance',
    'kızının sevgilisi': 'acquaintance',
    'kapıcı': 'acquaintance',
    'tanıdığı bir erkek': 'acquaintance',
    'tanıdık biri / akraba': 'acquaintance',
    'iş arkadaşi': 'acquaintance',
    'eski damat': 'acquaintance',
    'kizinin eski̇ kocasi': 'acquaintance',
    'kızının ni̇sanlisi': 'acquaintance',
    'oğlunun erkek arkadaşı': 'acquaintance',
    'kocasının iş arkadaşı': 'acquaintance',
    'arkadaşinin eski̇ kocasi': 'acquaintance',
    'annesinin eski kocasu': 'acquaintance',
    'tanıdığı erkek': 'acquaintance',
    'i̇ş arkadaşi': 'acquaintance',
    'iş arkadaşı': 'acquaintance',
    'kizinin ni̇sanlisi': 'acquaintance',
    'tanıığı bi erkek': 'acquaintance',
    'eski damadı': 'acquaintance',
    
    # Husband related
    'evli olduğu erkek': 'husband',
    'kocası': 'husband',
    'koca': 'husband',
    'kocasi': 'husband',
    'ayrı yaşadığı eşi': 'husband',
    'eşi': 'husband',
    'eş': 'husband',
    'evli olduğu erkek ve baba': 'husband',
    'evli olduğu erkek, baba, akraba': 'husband',
    'evli olduğu erkek ve tanıdık': 'husband',
    'baba, evli olduğu erkek, akraba': 'husband',
    'evli olduğu erkek ve akraba': 'husband',
    'şüpheli, koca-oğul': 'husband',
    'eşi ve eşinin ailesi': 'husband',
    
    # Former husband related
    'eskiden evli olduğu erkek': 'former_husband',
    'eski kocası': 'former_husband',
    'eski̇ kocasi': 'former_husband',
    'eski koca': 'former_husband',
    'boşandığı kocası': 'former_husband',
    'ayrı yaşadığı kocası': 'former_husband',
    'boşandığı erkek': 'former_husband',
    'eski eş': 'former_husband',
    'eski eşi': 'former_husband',
    
    # Partner related
    'birlikte olduğu erkek': 'partner',
    'sevgilisi': 'partner',
    'erkek arkadaş': 'partner',
    'nişanlı': 'partner',
    'dini nikahlı kocası': 'partner',
    'nişanlı olduğu erkek': 'partner',
    'sevgi̇li̇si̇': 'partner',
    'ni̇şanlisi': 'partner',
    'imam nikahlı kocası': 'partner',
    'dini nikahlı eşi': 'partner',
    'dini nikahla evli olduğu erkek': 'partner',
    "erkek arkadaşı" : "partner",
    "imam nikahlı erkek" : "partner",
    "imam nikahlı eşi" : "partner",
    "ni̇şanli" : "partner",
    "kizinin sevgi̇li̇si̇" : "partner",
    "nişanlısı" : "partner",
    "erkek arkadaiı" : "partner",
    "erkek arkdaşı" : "partner",
    
    # Former partner related
    'eskiden birlikte olduğu erkek': 'former_partner',
    'eski erkek arkadaş': 'former_partner',
    'eski sevgilisi': 'former_partner',
    'eski erkek arkadaşı': 'former_partner',
    'eski̇ sevgi̇li̇si̇': 'former_partner',
    'eski̇ erkek arkadaşi': 'former_partner',
    'eski̇ erkek arkadaş': 'former_partner',
    'imam nikahlı eski kocası': 'former_partner',
    "ayrıldığı erkek" : 'former_partner',
    "eski sevgili" : 'former_partner',
    "eski erkek arkaşı" : 'former_partner',
    
    # Son related
    'oğlu': 'son',
    'oğul': 'son',
    'üvey oğlu': 'son',
    'üvey oğul': 'son',
    
    # Stranger related
    'tanımıyor': 'stranger',
    'yabancı': 'stranger',
    'yabancılar': 'stranger',
    'tanımadığı erkek': 'stranger',
    'tanımadığı bir erkek': 'stranger',
    'tanımadığı kişi - minibüs şoförü': 'stranger',
    'tanımadığı biri': 'stranger',
    'onu kaçıran': 'stranger',
    '2 erkek': 'stranger',
    
    # Other
    'diğer': 'other',
    
    # Employer
    'işveren': 'employer',
    
    # Unknown
    'bi̇li̇nmi̇yor': 'unknown',
    'bilinmiyor': 'unknown',
    'ki̇mli̇ği̇ beli̇rlenmedi̇': 'unknown',
    'şüpheli ölüm': 'unknown',
    'sevgilisi olduğu iddia ediliyor': 'unknown',
    'aile kararı' : 'unknown',
    "nan" : "unknown"
}


femicide_data_frame['by_whom'] = femicide_data_frame['by_whom'].replace(corrections, regex=False)

femicide_data_frame["murderer_name"] = femicide_data_frame["murderer_name"].astype(str)

corrections = {
    "nan" : "unknown",
    "şüpheli ölüm" : "unknown"
}

femicide_data_frame['murderer_name'] = femicide_data_frame['murderer_name'].replace(corrections, regex=False)

femicide_data_frame["weapon"] = femicide_data_frame["weapon"].astype(str).str.lower().str.strip()

weapon_categories = {
    "firearm": ["ateşli silah", "ateşli", "ateşli̇ si̇lah", "tabanca", "ateşli̇ bi̇r si̇lah türü", "pompalı tüfek",
                "av tüfeği", "ateşli̇ si̇lah türü", "tüfek", "darp ve ateşli silah", "ruhsatlı tabanca",
                "ateşli silah - kesici alet", "dinamit", "boğma-ateşli silah", "pompali tüfek", "ateşli ve kesici",
                "ateşli̇si̇lah", "ateşli silah - ardından yakma", "ateşli silah ve kesici alet", "ruhsatsız silah"],
    "blade": ["kesici alet", "bıçak", "kesi̇ci̇ alet türü", "kesici/boğarak", "kesici alet-boğma",
              "ütü kablosuyla boğmuş, bıçaklamış", "kesici alet ve boğma", "boğma-kesici silah",
              "kesici alet-darp", "kesi̇ci̇ bi̇r alet türü", "biçak", "kesi̇ci̇ alet", "kesici", "kesici silah", "kesici alet türü"],
    "strangulation": ["boğularak?", "boğulma", "boğup kafasına çekiçle vurmuş 6 parçaya ayırmış", "boğma",
                      "asılarak", "boğularak", "boğarak"],
    "beating": ["darp edilerek", "i̇şkence", "sert bi̇r ci̇si̇mle darp edi̇lerek", "sert cisim kullanarak",
                "darp (başa sert cisimle vurularak)", "sert ci̇si̇m balyoz", "sert ci̇si̇m odun + testere",
                "sarımsak döveceği ve tornavida", "darp(kesik-ısırık-morluk)", "araçla ezme-darp", 
                "darp (duvara vurarak)", "vajinasında yırtılma", "sert cisim", "darp ederek", "darp", "işkence"],
    "chemical": ["zehirleme", "kimyasal madde"],
    "burning": ["yakıcı madde", "yanıcı sıvı", "benzin dökerek yakmış", "yanarak", "yakarak", "yakılarak",
                "kesici alet, boğulma, yanıcı madde"],
    "fall_from_height": ["balkondan atmış", "yüksekten atma", "yüksekten düşerek", "yüksekten düşme"],
    "other": ["araba ile üzerinden geçmek", "araçla ezerek", "çocuk yaşta doğum", "trafi̇k araci", "diğer",
              "zorla araca bindirip uçurumdan aşağı aracı sürerek"],
    "unknown": ["nan", "şüpheli", "intihar", "belirtilmemş", "şüpheli ölüm"]
}

for category, terms in weapon_categories.items():
    femicide_data_frame['weapon'].replace(terms, category, inplace=True)


corrections = {
    "Var" : "yes",
    "Yok" : "no",
    "Uzaklaştırma" : "yes",
    "uzaklaştırma" : "yes",
    "KORUMA ALDIKTAN BİR SÜRE SONRA BIRAKMIS." : "no",
    "Evli" : "unknown",
    "Bekar" : "unknown",
    "Boşanmış" : "unknown",
    "Evli (Ayrılmak Üzere)" : "unknown"
}
femicide_data_frame["protection_measure"] = femicide_data_frame["protection_measure"].replace(corrections, regex=False)

femicide_data_frame["protection_judicial_application"] = femicide_data_frame["protection_judicial_application"].astype(str).str.lower().str.strip()

filter_judicial_app = femicide_data_frame["protection_judicial_application"].str.contains("boşanma").fillna(False)
femicide_data_frame.loc[filter_judicial_app , 'judicial_application'] = femicide_data_frame.loc[filter_judicial_app, 'protection_judicial_application']

filter_judicial_app = femicide_data_frame["protection_judicial_application"].str.contains("şikayet").fillna(False)
femicide_data_frame.loc[filter_judicial_app , 'judicial_application'] = femicide_data_frame.loc[filter_judicial_app, 'protection_judicial_application']



protection_categories = {
    "yes": ["uzaklaştırma", "1", 1, "var", "uzaklaştırma kararı var", "koruma", "koruma kararı var", "koruması var", "Uzaklaştırma", "2 defa uzaklaştırma(boşanma davası)",
             "korunma talebi var", "koruma talebi- uzaklaştırma kararı almış", "uzaklaştırma almış", "evden uzaklaştırılma cezası almış", "uzaklaştırma cezası"],
    
    "no": ["yok", 0, "0", "tedbir kararı varmış cinayet günü bitmş"],
   
    "unknown": ["boşanma davası var", "boşanma aşamasında", "ayrı yaşıyorlar", "boşanmış", "şüpheli ölüm", "polise şikayette bulunmuş", "şikayetçi olmuş", 
                "boşanma davası", "boşanmak için avukatla görüşmeye gidilmiş", "rahatsız edildiği için şikayetçi olmuş", "boşanma davası açılmış", "adli başvuru", "nan"],
}

for category, terms in protection_categories.items():
    femicide_data_frame['protection_judicial_application'].replace(terms, category, inplace=True)


protection_is_null = femicide_data_frame["protection_measure"].isnull()
femicide_data_frame.loc[protection_is_null , 'protection_measure'] = femicide_data_frame.loc[protection_is_null, 'protection_judicial_application']


femicide_data_frame = femicide_data_frame.loc[:, ~femicide_data_frame.columns.str.contains('protection_judicial_application')]


judicial_app_categories = {
    "divorce_process" : ["Boşanma aşaması", "boşanma davası var", "boşanma aşamasında", "boşanma davası açılmış", "boşanmak için avukatla görüşmeye gidilmiş", "boşanma davası"],

    "police_report" : ["Polis veya Savcılığa şikayet", "polise şikayette bulunmuş", "şikayetçi olmuş", "rahatsız edildiği için şikayetçi olmuş", "2 defa uzaklaştırma(boşanma davası)"]
}

for category, terms in judicial_app_categories.items():
    femicide_data_frame['judicial_application'].replace(terms, category, inplace=True)


femicide_data_frame["pretext"] = femicide_data_frame["pretext"].astype(str).str.lower().str.strip()

pretext_categories = {
    "decision_making_autonomy": [
        "hayatına dair karar alma", "kendi hayatına dair kara almak istemesi", "boşanmak istediği için",
        "ilişkiyi sonlandırma", "boşanmak", "kendi hayatına dair karar vermek", "kendi hayatına dair karar alma",
        "boşanmak isteme", "kendi hayatına dair karar", "kendi hayatına dair karar almak istediği için",
        "ilişkisini sonlandırmak istediği için", "kadının kendi hayatına dair diğer kararlar (finansal/bedensel/sosyal)",
        "barışma isteğini kabul etmeme", "ayrılmak isteme", "kendi hayatına dair karar alması", "boşanma isteği",
        "ayrılmak", "kendi hayatına dair karar almak istediği içn", "boşanmak istemesi",
        "kadının evliliği sonlandırmak istemesi/girişimi", "boşanma", "kendi hayatına dair karar almak istmesi",
        "ilişkiyi sonlandırmak istemesi", "kadının başka kadını korumak istemesi", "kadının başka bir kadını korumak istemesi",
        "barışma isteğini reddettiği için", "ayrılmak istediği için", "ayrilmak", "ayrılma/reddetme",
        "kendisiyle birlikte olmak istememesi", "ayrılmak i̇stemesi", "ilişkiyi sonlandırmak", "barışma isteğine olumsuz sonuç alması",
        "kızının boşanmak istemesine destek vermesi", "kendi kararını verme", "ilişkiyi sonlandırma isteği",
        "kendi kararını vermeyi istemesi", "boşanma i̇stegi̇", "bosanma i̇stegi̇(tartisma)", "evli̇li̇k tekli̇fi̇ni̇ reddetmesi̇",
        "barışmak i̇stemesi", "kadının ilişkiyi sonlandırmak istemesi/girişimi", "ailesinin izni olmadan evlenme",
        "boşandığı için", "kadının yeğeninin, kızıyla birlikte olması", "kadının babası ile birlikte olması", "bosanma","kkhddk", "khdk",
        "kendi̇ hayatina dai̇r karar", "arkadaşlık teklifini reddettiği için", "telefon şifresini vermemek", "kkhk", "aldatıldığını iddia etmiş",
        "kendi hatına dair karar almak istemesi", "aşkına karşılık vermemesi", "evlenme teklifini ret", "ci̇nsel i̇li̇şki̇ye gi̇rmek i̇stememesi̇",
        "hizmet etmediği için", "kıskançlık?", "düğün tari̇hi̇ veri̇lmemesi̇", "eli̇f'i̇n bosanma davasi", "erkeklerle ilişki", "aldatma i̇ddi̇asi", "kıskançlık",
        "kkhdk", "ilişki teklifini kabul etmeme", "khddk - bedensel / sosyal", "bosanma i̇stegi̇", "ayrilmak i̇stemesi̇", "karısı ayrılmak istediği için", 
        "ayrılmak istemesi", "ayrılmak istemedi"
    ],
    "economic": [
        "ekonomik", "ekonomik sebepler", "tartışma - maddi sıkıntı", "para - khdk", "annesinin boşanma davası - mal paylaşımı", "boşanma davası - mal paylaşımı",
        "tartışma - ev tapusu",
    ],
    "hate_crime_homophobia": [
        "nefret", "nefret cinayeti", "homofobi/transfobi/bifobi", "transfobi", "trans kadın olması",
    ],
    "other": [
        "diğer", "şüpheli ölüm", "yanlışlıkla", "kş/csd", "kaçırılma ve tecavüz", "tecavüz", "silahını temizlemek", 
        "uyuşturucu satıcılarına tepki göstermek", "cinsel saldırıya direnmek", "kızını korumaya çalışırken", "şüheli ölüm", "çocuklarıyla ilgili",  
        "şaka", "torununu koruması", "ailevi nedenler", "töre", "kızını korumak istemesi", "erkek çocuk olarak doğmadığı için", "cinsel saldırı",
        "tecvüze direnme", "erken yaşta doğum", "**ci̇nsel taci̇ze karşı koyma",
        "başka kadını korumaya çalışması", "bogazi kesi̇li̇p magarada yakilmiş.", "bunalim/ci̇nnet",
        "uyuşturucu kullanimi yuzunden cikan tartisma", "başka bir kadını korumak istediği için",
        "evi̇ni̇n bahcesi̇nde ölü bulundu.", "babasinin si̇lahiyla i̇nti̇har", 
        "kiziyla tartişmasina müdahale etmesi̇", "tecavüze direnme", "tecavüze uğradığı için ?", 
        "(şüpheli intihar, soruşturma sürüyor)", "nedeni şüpheli cinayet", "uyusturucu kullanimi yuzunden cikan tartisma",
        "i̇ntikam (yanlış kişiyi vurdu)", "kadının oğlunun, kızıyla birlikte olması", "kati̇li̇n taci̇zi̇ne nezahat'i̇n di̇renmesi̇",
        "kati̇li̇n i̇şlemek i̇stedi̇ği̇ kadin ci̇nayeti̇ni̇ songül'ün engellemeye çalişmasi", 
        "şüpheli̇ ölüm", "kadının bir başka kadına karşı şiddeti engellemeye çalışması", "başka bir kadını koruma"
    ],
    "unknown" : [
        "bi̇li̇nmi̇yor", "tartışma", "tespt edilemeyen", "bilinmeyen neden", "kbbkşeç", "belirlenemeyen bir neden", "bi̇li̇nmeyen sebeple ai̇lesi̇yle tartisma esnasinda",
        "muhtemel ai̇le i̇çi̇ alinan ölüm karari", "tartişma ?", "htb", "tarti̇şma", "beli̇rlenemeyen neden", "tartişma", "nan"
    ]

}

for category, terms in pretext_categories.items():
    femicide_data_frame["pretext"].replace(terms, category, inplace=True)
    

femicide_data_frame["marital_status_of_women"] = femicide_data_frame["marital_status_of_women"].astype(str).str.lower().str.strip()


marital_status_categories = {
    "married" : [
        "ayrı yaşıyorlar", "evli̇ ayri yaşiyor", "boşanma aşamasında", "evli̇", "evli"
    ],
    "single" : [
        "nişanlı", "yok", "dini nikah", "boşanmış", "dini nikahlı", "bekar"
    ],
    "unknown" : [
        "kayıp ilanı verilmiş", "istanbulda kayıp ilanı verilmiş", "sonrasında intihar etmiş", "2 çocuk da yaralanmış", "sonrasında intihara kalkışmış",
        "tespit edlemeyen", "nan"
    ]
}

for category, terms in marital_status_categories.items():
    femicide_data_frame["marital_status_of_women"].replace(terms, category, inplace=True )



location_categories = {
    'home': [
        'Ev', 'ev', 'kendi evi', 'evde', 'kadının evi', 'evin bahçesi', 'kadının ailesinin evi', 
        'apartman boşluğu', 'evin balkonu', 'evinin balkonu', 'başkasının evi', 'yakınının evi', 
        'birlikte yaşadıkları ev', 'babasının evi', 'apart', 'köy evi', "apartmanda"
    ],
    'natural_spaces': [
        'Arazi', 'arazi', 'Park', 'park', 'Ormanlık alan', 'ormanlık alan', "bahçe", "gömülü",
        'Issız yer', 'Issız Yer', 'çalılık', 'Tarla', 'tarla', 'kırsal alan', 'sazlık alanda'
    ],
    'public_commercial_places': [
        'İş yeri', 'iş yeri', 'işyeri', 'kadının işyeri', 'Dükkan', 'dükkan', 'AVM', 'Hastane', "sığınma evi servisi", "sürücü kursu",
        'hastane', 'Okul', 'okul', 'Kafe', 'kafe', 'Diğer Kamusal Alan', 'Diğer kamusal alan', "Otel", "hocasının odası", "dinlenme tesisi", 
        'minibüs', 'otel', 'otel odası', 'otel barı/bahçesi', 'pasaj', 'düğün', 'ofis', 'belediye binası', "kadının çalıştığı markette",
        "kadın sığınma evi", "hastane otoparkı", "kadının çalıştığı markette"
    ],
    'transportation_areas': [
        'Sokak', 'sokak', 'sokakta', 'yol', 'yol kenarı', 'köprü', 'otobüs durağı', 'istasyon'
    ],
    'water_bodies': [
        'Su ve kenarı', 'Su ve Kenarı', 'dere kenarı', 'gölet', 'göl', 'deniz kıyısında', 'falezlerde', "çay kenarı",
        'su kuyusunda', 'su yatağında', 'kuyu', 'baraj', 'baraj gölü', 'Dere kenarı', 'dere kenarı', 'Göl', 'göl', 'Su ve Kenarı', 'Su ve kenarı', 
        'Dere yatağı', 'deniz kıyısında', 'nehri', 'deniz', 'ırmak', 'Sahil', 'sahilde', "dere yatağı", "sulama kanalı", "nehirde"
    ],
    'other_places': [
        'Araba', 'araba', 'arabada', 'Eğlence Mekanı', 'eğlence mekanı', 'Eğlence mekanı', 'pazar', 
        'bakkal', 'inşaat', 'sera içinde', 'boş arazi', 'boş bina', 'kayalık', "Diğer", "tır", "elektrik direği"
    ]
}


for category, terms in location_categories.items():
    femicide_data_frame['place_of_femicide'].replace(terms, category, inplace=True)


corrections = {
    "Var" : "yes",
    "Yok" : "no"
}

femicide_data_frame["women_employement_status"] = femicide_data_frame["women_employement_status"].replace(corrections, regex=False)

corrections = {
    "Var" : "yes",
    "Yok" : "no"
}

femicide_data_frame["women_disability_status"] = femicide_data_frame["women_disability_status"].replace(corrections, regex=False)

corrections = {
    "Suç Kaydı var"                 : "criminal_record",
    "Eski Mahkum"                   : "former_prisoner",
    "İzinli Çıkmış"                 : "released_on_leave",
    "Yargılanması Devam Ediyor"     : "undergoing_trial",
    "Denetimli Serbestlik"          : "probation",
    "Firari"                        : "fugitive",
    "Takipsizlik"                   : "dropped_charges",
    "Adli Kontrol Şartıyla Serbest" : "judicial_control",
    "Soruşturma Aşaması"            : "under_investigation",
    "Beraat"                        : "acquitted"
}

femicide_data_frame["murderer_judicial_record"] = femicide_data_frame["murderer_judicial_record"].replace(corrections, regex=False)

corrections = {
    "Var" : "yes",
    "Yok" : "no"
}

femicide_data_frame["other_killed_or_injured"] = femicide_data_frame["other_killed_or_injured"].replace(corrections, regex=False)



