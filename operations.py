import re
from pdfminer.high_level import extract_text


class Operation:
    def __init__(self, operation_date, currency_date, id_, description, type_, value, balance):
        self.operation_date = operation_date
        self.currency_date = currency_date
        self.id_ = id_
        self.description = description
        self.type_ = type_
        self.value = float(value.replace(',', '.').replace(' ', ''))
        self.balance = float(balance.replace(',', '.').replace(' ', ''))


def load_operations(file_path):
    pdf_text = extract_text(file_path)
    tables_begin = re.search('Opis operacji', pdf_text).span()[1]
    tables_end = re.search('Saldo końcowe', pdf_text).span()[0]
    table_content_raw_dirty = pdf_text[tables_begin:tables_end]

    while True:
        try:
            cut_begin = re.search('(Saldo do przeniesienia)|(Niniejszy dokument jest wydrukiem)',
                                  table_content_raw_dirty).span()[0]
            cut_end = re.search('Opis operacji\n\n', table_content_raw_dirty).span()[1]
            table_content_raw_dirty = table_content_raw_dirty[:cut_begin] + table_content_raw_dirty[cut_end:]
        except AttributeError:
            break

    table_content = re.split('\n\n(?=\d\d\.\d\d\.\d\d\d\d)', table_content_raw_dirty)[1:]
    data_lines = list(filter(lambda l: re.match('^.*\n\n\d{4}\w\w\d{11}', l), table_content))
    desc_lines = list(filter(lambda l: l not in data_lines, table_content))

    if len(data_lines) != len(desc_lines):
        raise Exception('Lines are not matching!')

    operations = []
    for data_line, desc_line in zip(data_lines, desc_lines):
        data_line = data_line.split('\n\n')
        desc_line = desc_line.split('\n\n')

        operation_date = data_line[0]
        currency_date = desc_line[0]
        id_ = data_line[1]
        description = desc_line[1]
        type_ = data_line[2]
        value = data_line[3]
        balance = data_line[4]

        operations.append(Operation(operation_date, currency_date, id_, description, type_, value, balance))

    return operations
