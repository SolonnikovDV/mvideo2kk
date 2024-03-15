import os
import logging
import psycopg2
import matplotlib.pyplot as plt
import pandas as pd

from ini import GP_CREDS, HISTORIC_ATTR, PATH_TO_TABLE_STAT

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_monthly_growth_info(table_name, date_column, date_type, conn_params):
    try:
        with psycopg2.connect(**conn_params) as conn, conn.cursor() as cur:
                if date_type == 'bigint':
                    # Используем деление на 1,000,000 для преобразования микросекунд в секунды
                    date_expression = f"TO_TIMESTAMP({date_column}::bigint / 1000000)"
                else:
                    date_expression = date_column

                # Добавляем условие для исключения записей (выбросы в данных) до 1 января 1971 года
                query = f"""
                SELECT DATE_TRUNC('month', {date_expression}) AS month,
                       COUNT(*) AS row_count
                FROM {table_name}
                WHERE {date_column} IS NOT NULL AND {date_expression} > '1971-01-01'
                GROUP BY 1
                ORDER BY 1;
                """
                cur.execute(query)
                results = cur.fetchall()
                return pd.DataFrame(results, columns=['Month', 'RowCount'])
    except Exception as e:
        logging.error(f"Error processing table {table_name} with date_column {date_column}: {e}")
        return pd.DataFrame(columns=['Month', 'RowCount'])



def plot_growth_info(df, table_name):
    output_dir = PATH_TO_TABLE_STAT
    os.makedirs(output_dir, exist_ok=True)

    plt.figure(figsize=(10, 6))
    plt.plot(df['Month'], df['RowCount'], marker='o', linestyle='-', color='b')
    plt.title(f'Monthly Row Growth for {table_name}')
    plt.xlabel('Month')
    plt.ylabel('Row Count')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/{table_name.replace('.', '_')}_row_growth.png")
    plt.close()


def get_table_size(table_name, conn_params):
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT pg_total_relation_size('{table_name}')")
            size_bytes = cur.fetchone()[0]
            size_mb = size_bytes / (1024 ** 2)
            return size_mb


def save_statistics(statistics, directory="table_stat"):
    os.makedirs(directory, exist_ok=True)
    df_stats = pd.DataFrame(statistics)
    df_stats.to_csv(f"{directory}/table_growth_statistics.csv", index=False)

    with open(f"{directory}/table_growth_statistics.md", 'w') as md_file:
        for stat in statistics:
            md_file.write(f"## {stat['table_name']}\n")
            for key, value in stat.items():
                if key != 'table_name':
                    md_file.write(f"- **{key}**: {value}\n")
            md_file.write("\n")


def calculate_statistics(df, table_name, conn_params):
    if df.empty:
        return None

    avg_row_count = round(df['RowCount'].mean())
    total_size_mb = get_table_size(table_name, conn_params)
    avg_monthly_size_mb = round(total_size_mb / len(df), 2)

    # Находим месяц с наибольшим приростом строк
    max_growth_month = df.loc[df['RowCount'].idxmax()]['Month'].strftime('%Y-%m')
    max_growth_count = round(df.loc[df['RowCount'].idxmax()]['RowCount'])

    # Оцениваем размер данных для месяца с наибольшим приростом строк
    # Предполагаем, что средний размер строки в мегабайтах постоянен
    if avg_row_count > 0:  # Предотвращаем деление на ноль
        avg_row_size_mb = total_size_mb / df['RowCount'].sum()
        max_growth_size_mb = round(avg_row_size_mb * max_growth_count, 2)
    else:
        max_growth_size_mb = 0

    formatted_avg_row_count = "{:,}".format(avg_row_count).replace(',', '_')
    formatted_avg_monthly_size_mb = "{:,}".format(avg_monthly_size_mb).replace(',', '_')
    formatted_max_growth_count = "{:,}".format(max_growth_count).replace(',', '_')
    formatted_max_growth_size_mb = "{:,}".format(max_growth_size_mb).replace(',', '_')

    statistics = {
        'table_name': table_name,
        'avg_monthly_rows': formatted_avg_row_count,
        'avg_monthly_size_mb': formatted_avg_monthly_size_mb,
        'max_growth_month': max_growth_month,
        'max_growth_count': formatted_max_growth_count,
        'max_growth_size_mb': formatted_max_growth_size_mb,
    }
    return statistics


def main(historic_attr, conn_params):
    statistics_list = []
    for table_info in historic_attr:
        table_name = table_info['table_name']
        details = table_info['details']
        date_column = details.get('create_date')
        date_type = details.get('dtype', 'timestamp')
        if date_column:
            df = get_monthly_growth_info(table_name, date_column, date_type, conn_params)
            if not df.empty:
                plot_growth_info(df, table_name)
                statistics = calculate_statistics(df, table_name, conn_params)
                if statistics:
                    statistics_list.append(statistics)

    if statistics_list:
        save_statistics(statistics_list)


if __name__ == "__main__":
    '''
    Пример словаря с описанием таблицы:
    
    HISTORIC_ATTR = [
        {
            'table_name': 'schema_name.table_name',
            'details': {
                'create_date': 'field_with_date',
                'update_date': 'field_with_date',
                'dtype': 'using_data_type'
            }
        },
    ]
    '''
    main(HISTORIC_ATTR, GP_CREDS)