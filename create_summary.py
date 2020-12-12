from operations import load_operations
import os

tables_dir = 'Wyciagi_PKO'
filename = os.listdir(tables_dir)[0]

operations = load_operations(f'{tables_dir}\\{filename}')

# Stats
groups = {'Lewiatan': list(filter(lambda op: 'SKLEP SPOZ.KULA' in op.description, operations)),
          'Biedronka': list(filter(lambda op: 'BIEDRONKA' in op.description, operations)),
          'Allegro': list(filter(lambda op: 'allegro.pl' in op.description, operations)),
          'BiletyMiejskie': list(filter(lambda op: 'PRZELEW ZAKUP BILETU' in op.type_, operations))}

for group, list_ in groups.items():
    print(group, sum(map(lambda l: float(l.value), list_)))

print()
