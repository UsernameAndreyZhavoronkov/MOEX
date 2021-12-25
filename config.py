# Колонки для первичной обработки таблиц.
colons = {
    'shares': ['BOARDID', 'TRADEDATE', 'SHORTNAME', 'SECID', 'NUMTRADES',
               'OPEN', 'LOW', 'HIGH', 'CLOSE', 'WAPRICE', 'VOLUME']
}

# Сообщение при неправильном вводе численных значений.
message1 = [
    'Ошибка, значение не распознано, введите только цифры ...',
    'От 0 до 9 ...',
    'Вверху на клавиатуре ...',
    'Или справа, где мышка ...',
]

# Сортируются активы с меньшем значением зависимости от приоритетного.
corr = 0


def restriction(eng='stock', mar='shares', bon='TQBR', tool_var=''):
    """ Метод принимает параметры торгуемого инструмента и если данные интересны возвращает 'True' иначе 'False'. """
    # if tool_var in ['SBER', 'VGSB', 'SBERP', 'SVAV']:
    if mar == 'shares' and bon == 'TQBR' and 'S' in tool_var:
        return True
    else:
        return False
