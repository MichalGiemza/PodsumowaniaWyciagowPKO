from functools import reduce

import pandas as pd

x = pd.read_csv('C:\\history_csv_20201222_182633.csv', encoding='Windows-1250')
x['RokMiesiac'] = x['Data operacji'].apply(lambda d: d[:7])
x['Opis'] = x.apply(lambda r: f"{r['Opis transakcji']} {r['Unnamed: 7']} {r['Unnamed: 8']} {r['Unnamed: 9']} {r['Unnamed: 10']} {r['Unnamed: 11']}", axis=1)

przedzialy = sorted(set(x['RokMiesiac']))
podsumowanie = pd.DataFrame(data={'RokMiesiac': przedzialy})

suma = x.groupby(['RokMiesiac']).aggregate({'Kwota': ['sum', 'max', 'min'], 'Saldo po transakcji': ['max', 'min']})
sum_all = x.groupby(['RokMiesiac']).aggregate({'Kwota': ['sum', 'max', 'min', 'count']})


# Queries
def query(df, q, case=False, regex=False):
    return df.loc[df.Opis.str.contains(q, case=case, regex=regex)]


def to_summary(df):
    return df.groupby(['RokMiesiac']).aggregate({'Kwota': ['sum', 'max', 'min', 'count']})


queries = {'Comarch': 'wynagrodzenie', 'Biedronka': 'biedronka', 'Lewiatan': 'SKLEP SPOZ.KULA M. KULA', 'Żabka': 'zabka',
           'Netflix': 'netflix', 'eLeclerc': 'eLeclerc', 'Allegro': 'allegro', 'UPC': 'upc', 'Czynsz': 'czynsz za',
           'Gaz': 'ZA GAZ', 'Prąd': '/KTR/', 'Serwer': 'ovh.pl', 'Dębowczyk': 'KAMIL GRZEGORZ DĘBOWCZYK',
           'Matka': 'OSTRÓWEK Tytuł: Czynsz', 'Intercity': 'intercity', 'Bilet MPK 0': 'ZAKUP BILETU KOMUNIKACYJNEGO',
           'Hostel': 'HOSTEL', 'Hotel': 'HOTEL', 'x-kom': 'x-kom.pl', 'Bilet MPK 1': 'MPK', 'Auchan': 'AUCHAN',
           'Leroy Merlin': 'Leroy Merlin', 'Lidl': 'LIDL', 'MaxiPizza': 'maxipizza', 'Kebab': 'kebab',
           'Rozliczenie wody': 'ROZLICZENIE WODY', 'Bankomat': 'PKO BP', 'Carrefour': 'CARREFOUR',
           'Stokrotka': 'Stokrotka', 'Plus': 'PLUS', 'McDonalds': 'McDonalds', 'MediaExpert': 'MEDIAEXPERT',
           'Restauracja': 'RESTAURACJA', 'Ruch': 'RUCH', 'Flixbus': 'flixbus.pl', 'OPSO': 'OPSO', 'Rossmann': 'ROSSMANN',
           'IKEA': 'IKEA', 'Jysk': 'JYSK', 'C&A': 'C&A', 'Media Markt': 'MEDIA MARKT', 'Apteka': 'Apteka',
           'WIZZ AIR': 'WIZZ AIR', 'Pizza': 'pizza', 'Bolt': 'BOLT.EU', 'Jula': 'JULA', 'Kaufland': 'KAUFLAND',
           'Da Grasso': 'DA GRASSO', 'e-podroznik': 'e-podroznik.pl', 'Makro': 'MAKRO', 'OpenSource': 'OPEN SOURCE',
           'Panek': 'PANEK', 'Castorama': 'CASTORAMA', 'Discord': 'DISCORD', 'Valve': 'Valve', 'Bilety ZTM': 'ZTM',
           'eventim': 'eventim', 'Wynagrodzenie': 'Wynagrodzenie', 'dotpay': 'dotpay.pl'}

groups = {'Wynagrodzenie': ['Comarch', 'Wynagrodzenie'],
          'Zakupy spoż.': ['Biedronka', 'Lewiatan', 'Żabka', 'eLeclerc', 'Auchan', 'Lidl', 'Carrefour', 'Stokrotka', 'Kaufland', 'Makro'],
          'Zakupy bud.': ['Leroy Merlin', 'IKEA', 'Jysk', 'Jula', 'Castorama'],
          'Zakupy RTV/AGD': ['MediaExpert', 'x-kom', 'Media Markt'],
          'Zakupy odzież.': ['C&A'],
          'Zakupy inne': ['Ruch', 'Apteka', 'Rossmann'],
          'Rachunki': ['Netflix', 'UPC', 'Czynsz', 'Gaz', 'Prąd', 'Serwer', 'Rozliczenie wody', 'Plus'],
          'Zakupy online': ['Allegro', 'Discord', 'Valve', 'eventim', 'dotpay'],
          'Przelewy': ['Dębowczyk', 'Matka'],
          'Transport międzymiastowy': ['Intercity', 'Flixbus', 'WIZZ AIR', 'e-podroznik'],
          'Transport miejski': ['Bilet MPK 0', 'Bilet MPK 1', 'Bolt', 'Panek', 'Bilety ZTM'],
          'Zakwaterowanie': ['Hostel', 'Hotel'],
          'Restauracje': ['MaxiPizza', 'Kebab', 'McDonalds', 'Restauracja', 'OPSO', 'Pizza', 'Da Grasso', 'OpenSource'],
          'Bankomat': ['Bankomat']}

results_transactions = {}
results = {}
remaining = x.copy()
for label, group_qs in groups.items():
    results_transactions[label] = pd.DataFrame(columns=['Data operacji', 'Data waluty', 'Typ transakcji', 'Kwota', 'Waluta', 'Saldo po transakcji', 'Opis transakcji', 'Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11', 'RokMiesiac', 'Opis'])
    for query_name in group_qs:
        results_transactions[label] = results_transactions[label].append(query(remaining, queries[query_name]))
        remaining = remaining[~remaining.index.isin(results_transactions[label].index)]
    results[label] = to_summary(results_transactions[label]).reindex_like(sum_all).fillna(0)

results_transactions['other_incomes'] = remaining.loc[remaining.Kwota > 0]
results_transactions['other_expenses'] = remaining.loc[remaining.Kwota < 0]
results['other_incomes'] = to_summary(results_transactions['other_incomes']).reindex_like(sum_all).fillna(0)
results['other_expenses'] = to_summary(results_transactions['other_expenses']).reindex_like(sum_all).fillna(0)

# Charts
import matplotlib.pyplot as plt


def summary_chart_expenses(results):
    expenses = {k: v['Kwota']['sum'].apply(lambda x: -min(0.0, x)) for k, v in results.items()}
    expenses = {k: v for k, v in filter(lambda el: el[1].sum() > 0.001, expenses.items())}

    labels = sum_all.index.values
    width = 0.35  # the width of the bars: can also be len(x) sequence
    fig, ax = plt.subplots()
    bottom = None
    for name, df in expenses.items():
        if bottom is None:
            ax.bar(labels, df.values, width, label=name, zorder=3)
            bottom = df.values
        else:
            ax.bar(labels, df.values, width, bottom=bottom, label=name, zorder=3)
            bottom += df.values
    ax.set_ylabel('Wydatki (zł)')
    ax.legend()
    plt.xticks(rotation=60)
    plt.grid(True, zorder=0)
    plt.ylim((0, max(bottom+500)))


def summary_chart_incomes(results):
    incomes = {k: v['Kwota']['sum'].apply(lambda x: max(0.0, x)) for k, v in results.items()}
    incomes = {k: v for k, v in filter(lambda el: el[1].sum() > 0.001, incomes.items())}

    labels = sum_all.index.values
    width = 0.35  # the width of the bars: can also be len(x) sequence
    fig, ax = plt.subplots()
    bottom = None
    for name, df in incomes.items():
        if bottom is None:
            ax.bar(labels, df.values, width, label=name, zorder=3)
            bottom = df.values
        else:
            ax.bar(labels, df.values, width, bottom=bottom, label=name, zorder=3)
            bottom += df.values
    ax.set_ylabel('Przychody (zł)')
    ax.legend()
    plt.xticks(rotation=60)
    plt.grid(True, zorder=0)
    plt.ylim((0, max(bottom+500)))


# Pie chart, where the slices will be ordered and plotted counter-clockwise:
month = '2020-05'
values = {k: v.loc[month].Kwota['sum'] for k, v in results.items()}


def list_transactions(df, month):
    return df.loc[df.RokMiesiac == month]


def expenses_chart(values, month):
    expenses = {k: -v for k, v in filter(lambda it: it[1] < 0, values.items())}
    expenses_sorted = sorted(expenses.items(), key=lambda el: el[1])

    labels = list(map(lambda k: f'{k} - {round(expenses[k], 2)}zł', map(lambda el: el[0], expenses_sorted)))
    vals = list(map(lambda el: el[1], expenses_sorted))

    fig1, ax1 = plt.subplots()
    ax1.pie(vals, labels=labels, autopct='%1.1f%%', shadow=False, startangle=0)
    ax1.axis('equal')
    ax1.set_title(f'Wydatki w {month}: {round(sum(vals), 2)} zł')


def incomes_chart(values, month):
    incomes = {k: v for k, v in filter(lambda it: it[1] > 0, values.items())}
    incomes_sorted = sorted(incomes.items(), key=lambda el: el[1])

    labels = list(map(lambda k: f'{k} - {round(incomes[k], 2)}zł', map(lambda el: el[0], incomes_sorted)))
    vals = list(map(lambda el: el[1], incomes_sorted))

    fig1, ax1 = plt.subplots()
    ax1.pie(vals, labels=labels, autopct='%1.1f%%', shadow=False, startangle=0)
    ax1.axis('equal')
    ax1.set_title(f'Przychody w {month}: {round(sum(vals), 2)} zł')


# expenses_chart(values, month)
# incomes_chart(values, month)
# print(list_transactions(results_transactions['other_expenses'], month).values)

summary_chart_incomes(results)
summary_chart_expenses(results)

plt.show()
