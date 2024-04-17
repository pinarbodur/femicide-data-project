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
                'Tarih':'tarih',
                'Ay':'ay',
                'İl':'il',
                'Kadının Adı':'kadinin_adi',
                'Kadının Yaşı':'kadinin_yasi',
                'Kadının Çalışma Durumu':'kadinin_calisma_durumu',
                'Bahane':'bahane',
                'Silah':'silah',
                'Katil / Şüphelinin Adı':'suphelinin_adi',
                'Katil / Şüphelinin Yakınlığı':'suphelinin_yakinligi',
                'Nerede':'nerede',
                'Öldürüldüğü anda 6284 var mı?':'olduruldugu_anda_6284_var_mi',
                'Çocuk Bilgisi':'cocuk_bilgisi',
                'Birliktelik Durumu':'birliktelik_durumu'
                }

    
    data.rename(columns=COLUMN_MAP, inplace=True)
    return data

