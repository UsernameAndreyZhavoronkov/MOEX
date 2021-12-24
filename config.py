colons = {
    'shares': ['BOARDID', 'TRADEDATE', 'SHORTNAME', 'SECID', 'NUMTRADES',
               'OPEN', 'LOW', 'HIGH', 'CLOSE', 'WAPRICE', 'VOLUME']
}

def restriction(eng='stock', mar='shares', bon='TQBR', tool_var=''):
    """ Метод принимает параметры торгуемого инструмента и если данные интересны возвращает 'True' иначе 'False'. """
    if tool_var in ['SBER', 'VGSB', 'SBERP', 'SVAV']:
        return True
    else:
        return False
