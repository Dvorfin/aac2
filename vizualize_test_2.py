import matplotlib.pyplot as plt

# Чтение файла
with open('simulation_results.csv', 'r') as file:
    lines = file.readlines()

# Разделение данных на секции
sections = {}
current_section = None

for line in lines:
    line = line.strip()
    if line.startswith('Node'):
        current_section = line
        sections[current_section] = []
    elif line and current_section:  # Пропускаем пустые строки
        sections[current_section].append(line)

# Преобразование данных в словари для удобства работы
node_data = {}
for section, data in sections.items():
    headers = data[0].split(',')
    rows = [list(map(float, row.split(','))) for row in data[1:]]
    node_data[section] = {headers[i]: [row[i] for row in rows] for i in range(len(headers))}





# Создание графика
plt.figure(figsize=(12, 6))
for section, data in node_data.items():
    if 'Compute Load History' in section:
        time = data['Time (seconds)']
        load = data['Load (FLOPS)']
        plt.plot(time, load, label=section)

plt.title('Загрузка вычислительной мощности нод со временем')
plt.xlabel('Время (секунды)')
plt.ylabel('Загрузка (FLOPS)')
plt.legend()
plt.grid(True)
plt.show()




# Создание графика
plt.figure(figsize=(12, 6))
for section, data in node_data.items():
    if 'Network Load History' in section:
        time = data['Time (seconds)']
        load = data['Network Load (Bytes)']
        plt.plot(time, load, label=section)

plt.title('Загрузка сети нод со временем')
plt.xlabel('Время (секунды)')
plt.ylabel('Загрузка сети (Байты)')
plt.legend()
plt.grid(True)
plt.show()








# Создание графика
plt.figure(figsize=(12, 6))

for section, data in node_data.items():
    if 'Running Tasks History' in section:
        time = data['Time (seconds)']
        running_tasks = data['Running Tasks Count']
        plt.plot(time, running_tasks, label=section)

plt.title('Количество задач на каждой ноде от времени')
plt.xlabel('Время (секунды)')
plt.ylabel('Количество задач')
plt.legend()
plt.grid(True)
plt.show()



# Общие метрики из первой строки файла
total_created_tasks = float(lines[1].split(',')[1])
total_rejected_tasks = float(lines[1].split(',')[2])
# Создание графика
labels = ['Созданные задачи', 'Отклоненные задачи']
values = [total_created_tasks, total_rejected_tasks]

plt.figure(figsize=(8, 6))
plt.bar(labels, values, color=['blue', 'red'])

plt.title('Общее количество созданных и отклоненных задач')
plt.ylabel('Количество задач')
plt.grid(axis='y')
plt.show()