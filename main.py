import apirestmoex
import analtydata

if __name__ == '__main__':
    # apirestmoex.run_module()
    c = []
    for i in range(3):
        i += 1
        c.append(f'data/enter/stock_shares_TQBR_SBER_0000{i}.csv')
    analtydata.data_get(c)

