import config

import time
from colorama import Fore


def dialog(cyc_run):
    cortege = None
    en_prof = None
    en_sum = None
    print(Fore.GREEN + 'Введите допустимое значение инвестиционных рисков в процентах.')
    print('Рекомендуемое значение 1 - 20 %')
    print('Для завершения работы введите: ' + Fore.YELLOW + 'quit')
    print(Fore.GREEN + 'Чтобы перейти в режим запроса графиков введите: ' + Fore.YELLOW + 'graph')
    while True:
        print(Fore.LIGHTMAGENTA_EX + '$$$ ' + Fore.WHITE, end='')
        en_risk = input()
        en_risk, s1, s2 = en_risk.partition('%')
        if (en_risk in 'quit') or (en_risk in 'graph'):
            cyc_run = False
            break
        try:
            en_risk = int(en_risk)
        except ValueError:
            for print_x in config.message1:
                print(Fore.LIGHTRED_EX + print_x)
                time.sleep(1)
            continue
        if en_risk > 100:
            print(Fore.GREEN + 'Это слишком рискованно, давайте чуть уменьшим риски ...')
        else:
            break
    if cyc_run:
        print(Fore.GREEN + 'Введите желаемое значение инвестиционных доходов в процентах.')
        print('Рекомендуемое значение 1 - 20 %')
        while True:
            print(Fore.LIGHTMAGENTA_EX + '$$$ ' + Fore.WHITE, end='')
            en_prof = input()
            en_prof, s1, s2 = en_prof.partition('%')
            if (en_prof in 'quit') or (en_prof in 'graph'):
                cyc_run = False
                break
            try:
                en_prof = int(en_prof)
            except ValueError:
                for print_x in config.message1:
                    print(Fore.LIGHTRED_EX + print_x)
                    time.sleep(1)
                continue
            if en_prof > 100:
                print(Fore.GREEN + 'Маловероятно, надо бы попроще ...')
            else:
                break
    if cyc_run:
        print(Fore.GREEN + 'Введите значение суммы инвестиций в рублях.')
        print('Рекомендуемое значение более 2000')
        while True:
            print(Fore.LIGHTMAGENTA_EX + '$$$ ' + Fore.WHITE, end='')
            en_sum = input()
            en_sum, s1, s2 = en_sum.partition('р')
            if (en_sum in 'quit') or (en_sum in 'graph'):
                cyc_run = False
                break
            try:
                en_sum = int(en_sum)
            except ValueError:
                for print_x in config.message1:
                    print(Fore.LIGHTRED_EX + print_x)
                    time.sleep(1)
                continue
            if en_sum < 2000:
                print(Fore.GREEN + 'Это не серьёзно, накинь ещё ...')
            else:
                break
    if cyc_run:
        print(Fore.GREEN + 'Собираем оптимальный торговый портфель в соответствии с параметрами:')
        print('допустимый риск ' + Fore.YELLOW + f'/ {en_risk} % /' + Fore.GREEN + ', прибыль ' + Fore.YELLOW +
              f'/ {en_prof} % /' + Fore.GREEN + ', сумма инвестиций ' + Fore.YELLOW + f'/ {en_sum} руб. /')
        cortege = (en_risk, en_prof, en_sum)
    else:
        for itr in (en_risk, en_prof, en_sum):
            if str(itr) in 'graph':
                cortege = 'g'
                cyc_run = True
    return cyc_run, cortege


def dialog2(gr_dir):
    while True:
        print(Fore.GREEN + 'Введите код актива.')
        print('Чтобы задать даты, используйте формат: ' + Fore.YELLOW + 'Код актива*Дата начала*Дата конца')
        print(Fore.GREEN + 'Чтобы перейти подбора инвестиций введите: ' + Fore.YELLOW + 'back')
        print(Fore.LIGHTMAGENTA_EX + '$$$ ' + Fore.WHITE, end='')
        graph = input()
        graph, s1, date_ = graph.partition('*')
        if graph in 'back':
            break
        date_start, s1, date_end = date_.partition('*')
        for itr in gr_dir:
            if graph in itr:
                print(Fore.LIGHTMAGENTA_EX + 'График ' + Fore.YELLOW + f'[{itr}]' + Fore.LIGHTMAGENTA_EX +
                      ' с даты --> ' + Fore.YELLOW + f'[{date_start}]' + Fore.LIGHTMAGENTA_EX + ' по дату --> ' +
                      Fore.YELLOW + f'[{date_end}]' + Fore.LIGHTMAGENTA_EX + ' строится разработчиком.')
                break
        else:
            print(Fore.LIGHTRED_EX + f'К сожалению по активу ' + Fore.YELLOW + f'[{graph}]' +
                  Fore.LIGHTRED_EX + ' нет информации для построения графика.')



