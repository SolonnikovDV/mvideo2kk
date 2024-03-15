import psycopg2

from ini import GP_CREDS, TABLE_NAMES


def get_table_info_gp(table_name, conn_params: dict):
    """Получение информации о таблице в GreenPlum."""
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            # Получаем количество строк
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            rows_count = cur.fetchone()[0]
            # Форматируем количество строк с разделителями тысяч
            formatted_rows_count = "{:_}".format(rows_count)
            # Получаем размер таблицы в байтах и конвертируем в гигабайты, округляя до десятых
            cur.execute(f"SELECT pg_total_relation_size('{table_name}')")
            size_bytes = cur.fetchone()[0]
            size_gb = round(size_bytes / (1024 ** 3), 2)  # Округление до двух знаков после запятой
            return formatted_rows_count, size_gb


def pretty_report(table_names: list, conn_params: dict, filename='gp_tables_size_info.md'):
    details = {}
    total_size = 0
    for table_name in table_names:
        rows_count, size_gb = get_table_info_gp(table_name, conn_params)
        details[table_name] = {"rows_count": rows_count, "size_gb": size_gb}
        total_size += size_gb

    with open(filename, 'w') as file:
        file.write("# GreenPlum Tables Size Report\n\n")
        file.write("## Summary\n")
        file.write(f"- Tables count: {len(table_names)}\n")
        file.write(f"- Total size (GB): {round(total_size, 2)}\n\n")
        file.write("## Details\n")
        # Заголовки таблицы
        file.write("| Table Name | Rows Count | Size (GB) |\n")
        file.write("|------------|------------|-----------|\n")
        # Данные таблицы
        for table, info in details.items():
            file.write(f"| {table} | {info['rows_count']} | {info['size_gb']} |\n")

if __name__ == '__main__':
    pretty_report(TABLE_NAMES, GP_CREDS)