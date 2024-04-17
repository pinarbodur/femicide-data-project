if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    COLUMN_MAP = {
                'Haber Linki' : 'informant_of_data',
                'Tarih':'historical_date',
                'Ay':'month_of_femicide',
                'İl':'province_of_femicide',
                'Kadının Adı':'women_name',
                'Kadının Yaşı':'women_age',
                'Kadının Çalışma Durumu':'women_employement_status',
                'Engel Durumu' : 'women_disability_status',
                'Bahane':'pretext',
                'Silah':'weapon',
                'Katil / Şüphelinin Adı':'murderer_name',
                'Katil / Şüphelinin Yaşı' : 'murderer_age',
                'Katil / Şüphelinin Yakınlığı':'by_whom',
                'Nerede':'place_of_femicide',
                'Öldürüldüğü anda 6284 var mı?':'protection_measure',
                'Çocuk Bilgisi':'child_info',
                'Birliktelik Durumu':'marital_status_of_women'
                }

    
    data.rename(columns=COLUMN_MAP, inplace=True)
    return data

