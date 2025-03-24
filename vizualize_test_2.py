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


plt.title('Загрузка вычислительной мощности нод со временем', fontweight='bold')
plt.xlabel('Время (секунды)', fontweight='bold')
plt.ylabel('Загрузка (FLOPS)', fontweight='bold')
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

plt.title('Загрузка сети нод со временем', fontweight='bold')
plt.xlabel('Время (секунды)', fontweight='bold')
plt.ylabel('Загрузка сети (Байты)', fontweight='bold')
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

plt.title('Количество задач на каждой ноде от времени', fontweight='bold')
plt.xlabel('Время (секунды)', fontweight='bold')
plt.ylabel('Количество задач', fontweight='bold')
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

plt.title('Общее количество созданных и отклоненных задач', fontweight='bold')
plt.ylabel('Количество задач', fontweight='bold')
plt.grid(axis='y')
plt.show()











# Посчитаем статичтические данные
average_flops_consumption = dict()
average_ethernet_consumption = dict()
average_tasks_on_node = dict()


for section, data in node_data.items():
    if 'Compute Load History' in section:
        if section in average_flops_consumption:
            load = data['Load (FLOPS)']
            average_flops_consumption[section].append(load)
        else:
            load = data['Load (FLOPS)']
            average_flops_consumption[section] = [load,]

    if 'Network Load History' in section:
        if section in average_ethernet_consumption:
            load = data['Network Load (Bytes)']
            average_ethernet_consumption[section].append(load)
        else:
            load = data['Network Load (Bytes)']
            average_ethernet_consumption[section] = [load,]

    if 'Running Tasks History' in section:
        if section in average_tasks_on_node:
            load = data['Running Tasks Count']
            average_tasks_on_node[section].append(load)
        else:
            load = data['Running Tasks Count']
            average_tasks_on_node[section] = [load,]


for node in average_flops_consumption.keys():
    #print(average_flops_consumption[node][0])
    print(f"Average load of {node} is {round(sum(average_flops_consumption[node][0]) / len(average_flops_consumption[node][0]), 2)} %")


for node in average_ethernet_consumption.keys():
    print(f"Average ethernet of {node} is {round(sum(average_ethernet_consumption[node][0]) / len(average_ethernet_consumption[node][0]), 2)} %")

for node in average_tasks_on_node.keys():
    print(
        f"Average tasks of {node} is {round(sum(average_tasks_on_node[node][0]) / len(average_tasks_on_node[node][0]), 2)} piece")

print(f"Total created tasks: {total_created_tasks}, \nTotal rejected tasks: {total_rejected_tasks}")


# Вычисление среднего количества задач
total_weighted_tasks = 0  # Сумма "время-задачи"
total_time = 0  # Общая длительность


for section, data in node_data.items():
    if 'Running Tasks History' in section:
        time = data['Time (seconds)']
        running_tasks = data['Running Tasks Count']

        print()
        for i in range(len(time) - 1):
            t_i = time[i]
            t_next = time[i+1]
            n_i = running_tasks[i]
            n_next = running_tasks[i + 1]

            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_tasks += n_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительност

        weighted_avg = total_weighted_tasks / total_time

        print(f"Среднее взвешенное: {weighted_avg:.6f}")