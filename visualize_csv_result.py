import pandas as pd
import matplotlib.pyplot as plt

# Чтение данных из CSV-файла
filename = "node_results.csv"
data = pd.read_csv(filename)
#
# Создание столбчатых диаграмм для каждого параметра
parameters = [
    "Weighted Load (%)",
    "Weighted Network Load (%)",
    "Weighted Tasks Load (pieces)",
    "Total Calculated Tasks"
]

plt.figure(figsize=(15, 10))  # Общий размер графика

for i, param in enumerate(parameters, 1):
    plt.subplot(2, 2, i)  # Размещение графиков в сетке 2x2
    plt.bar(data["Node"], data[param], color='skyblue', edgecolor='black')

    # Настройка заголовков и осей
    plt.title(param, fontweight='bold')
    plt.xlabel("Нода", fontweight='bold')
    plt.ylabel(param, fontweight='bold')

    # Добавление сетки
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Поворот подписей на оси X для удобства чтения
    plt.xticks(data["Node"], rotation=45)

    if param == "Weighted Tasks Load (pieces)":
        # Поднятие нижней границы оси Y
        plt.ylim(min(data[param]) - 0.1, max(data[param]) + 0.02)

# Автоматическая настройка макета
plt.tight_layout()

# Показать графики
plt.show()