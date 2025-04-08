import pandas as pd
import os

# Путь к папке с CSV-файлами
folder_path = "../results — копия/configuration_4/WRR"

# Получаем список всех CSV-файлов в папке
csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]

# Создаем пустой DataFrame для хранения сумм значений
summary_df = None

# Читаем каждый CSV-файл и суммируем значения
for file in csv_files:
    df = pd.read_csv(file)

    # Если summary_df еще не создан, инициализируем его
    if summary_df is None:
        summary_df = df.copy()
        # Обнуляем все значения, кроме столбца Node
        for col in summary_df.columns:
            if col != 'Node':
                summary_df[col] = 0
    # Суммируем значения из каждого файла
    for col in df.columns:
        if col != 'Node':
            summary_df[col] += df[col]

# Вычисляем средние значения
num_files = len(csv_files)
for col in summary_df.columns:
    if col != 'Node':
        summary_df[col] /= num_files

# Сохраняем результат в новый CSV-файл
output_file = os.path.join(folder_path, "average_results.csv")
summary_df.to_csv(output_file, index=False)

print(f"Средние значения сохранены в файл: {output_file}")