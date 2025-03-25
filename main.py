import threading
import queue
import time
import random
import logging
import csv

from node import Node
from task_distributor import RoundRobin, WeightedRoundRobin, LeastConnection, WeightedLeastConnection
from edge_device import EdgeDevice

# Настройка логирования
# для записи логов в файл:
logging.basicConfig(filename='simulation.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# для выводв логов в консоль:
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def save_data_to_csv(nodes):
    """Похоже на функцию calc_tests_results, но с сохранением данных в csv"""

    node_data = []      # список, который будет записываться в csv

    for node in nodes:
        current_node_data = {}

        total_weighted_load = 0
        total_weighted_network_load = 0
        total_weighted_tasks_load = 0
        total_time = 0

        current_node_data['Node'] = node.node_id

        # среднее взвешенное по нагрузке с учетом активной работы сервера
        filtered_data = [(t, L) for t, L in node.load_history if L > 0]
        for i in range(len(filtered_data) - 1):  # (0.2052457332611084, 80.0), (0.2057654857635498, 75.0),
            t_i, l_i = filtered_data[i]
            t_next, l_next = filtered_data[i + 1]

            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_load += l_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительность

        if total_weighted_load == 0:
            current_node_data['Weighted Load (%)'] = 0
        else:
            weighted_avg = total_weighted_load / total_time
            current_node_data['Weighted Load (%)'] = round(weighted_avg, 4)

        total_time = 0

        # среднее взвешенное по нагрузке сети с учетом активной работы сервера
        filtered_data = [(t, L) for t, L in node.network_load_history if L > 0]
        for i in range(len(filtered_data) - 1):  # (0.2052457332611084, 80.0), (0.2057654857635498, 75.0),
            t_i, l_i = filtered_data[i]
            t_next, l_next = filtered_data[i + 1]
            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_network_load += l_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительность

        if total_weighted_network_load == 0:
            current_node_data['Weighted Network Load (%)'] = 0
        else:
            weighted_avg = total_weighted_network_load / total_time
            current_node_data['Weighted Network Load (%)'] = round(weighted_avg, 4)

        total_time = 0

        # среднее взвешенное по количеству задач с учетом активной работы сервера
        filtered_data = [(t, L) for t, L in node.running_tasks_history if L > 0]
        for i in range(len(filtered_data) - 1):  # (0.2052457332611084, 80.0), (0.2057654857635498, 75.0),
            t_i, l_i = filtered_data[i]
            t_next, l_next = filtered_data[i + 1]
            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_tasks_load += l_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительность

        if total_weighted_tasks_load == 0:
            current_node_data['Weighted Tasks Load (pieces)'] = 0
        else:
            weighted_avg = total_weighted_tasks_load / total_time
            current_node_data['Weighted Tasks Load (pieces)'] = round(weighted_avg, 4)

        current_node_data['Total Calculated Tasks'] = node.done_tasks_count
        node_data.append(current_node_data)

    # Имя файла для сохранения
    filename = "node_results.csv"

    # Запись данных в CSV
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        # Определяем заголовки
        fieldnames = ["Node", "Weighted Load (%)", "Weighted Network Load (%)",
                      "Weighted Tasks Load (pieces)", "Total Calculated Tasks"]

        # Создаем объект writer
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # Записываем заголовки
        writer.writeheader()

        # Записываем данные для каждой ноды
        for data in node_data:
            writer.writerow(data)

    print(f"Данные успешно сохранены в файл: {filename}")


def calc_tests_results(nodes, total_created_tasks, total_rejected_tasks, simulation_duration):
    """Функция отписывает результаты в консоль"""
    total_weighted_load_list = []       # нагрузка серверов в активное время

    print("Nodes params:\n---------")
    for node in nodes:
        print("---------")
        print(f"Node {node.node_id}\nPower flops: {node.compute_power_flops}\n"
              f"Network bandwidth: {node.bandwidth_bytes}\n"
              f"Delay before executing task: {node.delay_seconds} sec\n"
              f"Down time: {node.downtime_seconds} sec")
        print("---------")
    print()

    total_calculated_tasks = 0

    print(f"---------")
    for node in nodes:
        total_weighted_load = 0
        total_weighted_network_load = 0
        total_weighted_tasks_load = 0
        total_time = 0

        # среднее взвешенное по нагрузке
        for i in range(len(node.load_history) - 1):  # (0.2052457332611084, 80.0), (0.2057654857635498, 75.0),
            t_i, l_i = node.load_history[i]
            t_next, l_next = node.load_history[i+1]
            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_load += l_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительность

        if total_weighted_load == 0:
            print(f"Weighted load of Node {node.node_id}: 0 %")
        else:
            weighted_avg = total_weighted_load / total_time
            print(f"Weighted load of Node {node.node_id}: {weighted_avg:.4f} %")
        total_time = 0
        total_weighted_load = 0

        # среднее взвешенное по нагрузке с учетом активной работы сервера
        filtered_data = [(t, L) for t, L in node.load_history if L > 0]
        for i in range(len(filtered_data) - 1):  # (0.2052457332611084, 80.0), (0.2057654857635498, 75.0),
            t_i, l_i = filtered_data[i]
            t_next, l_next = filtered_data[i + 1]

            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_load += l_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительность

        if total_weighted_load == 0:
            print(f"Weighted load of active time Node {node.node_id}: 0 %")
            total_weighted_load_list.append(0)
        else:
            weighted_avg = total_weighted_load / total_time
            total_weighted_load_list.append(round(weighted_avg, 4))
            print(f"Weighted load of active time Node {node.node_id}: {weighted_avg:.4f} %")
        total_time = 0

        # среднее взвешенное по нагрузке сети
        for i in range(len(node.network_load_history) - 1):  # (0.2052457332611084, 80.0), (0.2057654857635498, 75.0),
            t_i, l_i = node.network_load_history[i]
            t_next, l_next = node.network_load_history[i+1]
            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_network_load += l_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительность

        if total_weighted_network_load == 0:
            print(f"Weighted network load of Node {node.node_id}: 0 %")
        else:
            weighted_avg = total_weighted_network_load / total_time
            print(f"Weighted network load of Node {node.node_id}: {weighted_avg:.4f} %")
        total_time = 0
        total_weighted_network_load = 0

        # среднее взвешенное по нагрузке сети с учетом активной работы сервера
        filtered_data = [(t, L) for t, L in node.network_load_history if L > 0]
        for i in range(len(filtered_data) - 1):  # (0.2052457332611084, 80.0), (0.2057654857635498, 75.0),
            t_i, l_i = filtered_data[i]
            t_next, l_next = filtered_data[i + 1]
            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_network_load += l_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительность

        if total_weighted_network_load == 0:
            print(f"Weighted network load of active time Node {node.node_id}: 0 %")
        else:
            weighted_avg = total_weighted_network_load / total_time
            print(f"Weighted network load of active time Node {node.node_id}: {weighted_avg:.4f} %")
        total_time = 0

        # среднее взвешенное по количеству задач
        for i in range(len(node.running_tasks_history) - 1):  # (0.2052457332611084, 80.0), (0.2057654857635498, 75.0),
            t_i, l_i = node.running_tasks_history[i]
            t_next, l_next = node.running_tasks_history[i + 1]
            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_tasks_load += l_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительность

        if total_weighted_tasks_load == 0:
            print(f"Weighted tasks load of Node {node.node_id}: 0 pieces")
        else:
            weighted_avg = total_weighted_tasks_load / total_time
            print(f"Weighted tasks load of Node {node.node_id}: {weighted_avg:.4f} pieces")
        total_time = 0
        total_weighted_tasks_load = 0

        # среднее взвешенное по количеству задач с учетом активной работы сервера
        filtered_data = [(t, L) for t, L in node.running_tasks_history if L > 0]
        for i in range(len(filtered_data) - 1):  # (0.2052457332611084, 80.0), (0.2057654857635498, 75.0),
            t_i, l_i = filtered_data[i]
            t_next, l_next = filtered_data[i + 1]
            delta_t = t_next - t_i  # Длительность интервала
            total_weighted_tasks_load += l_i * delta_t  # Вклад в "время-задачи"
            total_time += delta_t  # Вклад в общую длительность

        if total_weighted_tasks_load == 0:
            print(f"Weighted tasks load of active time Node {node.node_id}: 0 pieces")
        else:
            weighted_avg = total_weighted_tasks_load / total_time
            print(f"Weighted tasks load of active time Node {node.node_id}: {weighted_avg:.4f} pieces")
        total_time = 0
        total_weighted_tasks_load = 0

        print(f"Node {node.node_id} calculated {node.done_tasks_count} tasks")
        total_calculated_tasks += node.done_tasks_count
        print(f"---------")

    print(f"Total tasks calculated: {total_calculated_tasks}")
    print(f"Total created tasks: {total_created_tasks}")
    print(f"Total rejected tasks: {total_rejected_tasks}")
    print(f"Percent of calculated tasks: {(total_calculated_tasks / total_created_tasks) * 100:.4f} %")
    print(f"RPS = {total_calculated_tasks} / {simulation_duration} = {(total_calculated_tasks / simulation_duration):4f}")
    print(f"Simulation duration: {simulation_duration}")
    print(total_weighted_load_list)



def save_results_to_csv(nodes, total_created_tasks, total_rejected_tasks, simulation_duration):
    """
    Сохраняет результаты симуляции в CSV-файл.

    :param nodes: Список нод.
    :param total_created_tasks: Общее количество созданных задач.
    :param total_rejected_tasks: Общее количество отклоненных задач.
    :param simulation_duration: Длительность симуляции.
    """
    filename = "simulation_results.csv"
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Simulation Duration", "Total Created Tasks", "Total Rejected Tasks"])
        writer.writerow([simulation_duration, total_created_tasks, total_rejected_tasks])

        # Загрузка вычислений, сети и количество задач для каждой ноды
        for node in nodes:
            writer.writerow([])
            writer.writerow([f"Node {node.node_id} - Compute Load History"])
            writer.writerow(["Time (seconds)", "Load (FLOPS)"])
            writer.writerows(node.load_history)

            writer.writerow([f"Node {node.node_id} - Network Load History"])
            writer.writerow(["Time (seconds)", "Network Load (Bytes)"])
            writer.writerows(node.network_load_history)

            writer.writerow([f"Node {node.node_id} - Running Tasks History"])
            writer.writerow(["Time (seconds)", "Running Tasks Count"])
            writer.writerows(node.running_tasks_history)


        time.sleep(16)


    logging.info(f"Simulation results saved to {filename}")


def calc_weights(nodes: list):
    """Функция для выичлсения веса нода перед запуском теста для отладки"""

    weights = []
    # считаем ненормализованные веса
    for node in nodes:
        flops, delay, bandwidth, fp = (node.compute_power_flops,
                                       node.delay_seconds,
                                       node.bandwidth_bytes,
                                       node.failure_probability)
        w = (flops + bandwidth) / (delay * 1000 + fp)
        weights.append(w)
        print(f"Weight of Node {node.node_id}: {w}")

    normalized_nodes_weights = []

    # Перещитываем веса (нормализуем в диапазон 1-10)

    max_weight = max(weights)
    min_weight = min(weights)

    for i in range(len(weights)):
        normalized_weight = 1 + 9 * ((weights[i] - min_weight) / (max_weight - min_weight))
        normalized_nodes_weights.append(normalized_weight)
        print(f"Node {i+1} normalized weight = {normalized_weight}")


#  config of simulation
simulation_duration = 15  # Длительность симуляции в секундах



if __name__ == "__main__":
    # Создаем ноды
    nodes = [
        Node(node_id=1, compute_power_flops=1000, delay_seconds=0.1, bandwidth_bytes=2000, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=2, compute_power_flops=405, delay_seconds=0.1, bandwidth_bytes=2000, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=3, compute_power_flops=404, delay_seconds=0.1, bandwidth_bytes=2000, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=4, compute_power_flops=403, delay_seconds=0.1, bandwidth_bytes=1500, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=5, compute_power_flops=800, delay_seconds=0.1, bandwidth_bytes=1000, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=6, compute_power_flops=1000, delay_seconds=0.1, bandwidth_bytes=500, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=7, compute_power_flops=820, delay_seconds=0.1, bandwidth_bytes=600, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=8, compute_power_flops=920, delay_seconds=0.1, bandwidth_bytes=1000, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=9, compute_power_flops=700, delay_seconds=0.1, bandwidth_bytes=700, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=10, compute_power_flops=880, delay_seconds=0.1, bandwidth_bytes=2000, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=11, compute_power_flops=396, delay_seconds=0.1, bandwidth_bytes=2000, failure_probability=0.2,
             downtime_seconds=4),
        Node(node_id=12, compute_power_flops=395, delay_seconds=0.1, bandwidth_bytes=2000, failure_probability=0.2,
             downtime_seconds=4)
    ]

    calc_weights(nodes)


    # Устанавливаем start_time для всех нод
    start_time = time.time()
    for node in nodes:
        node.start_time = start_time

    # Создаем дистрибьютор задач
    distributor = RoundRobin(nodes)

    # печатаем название класса
    class_name = type(distributor).__name__
    print(class_name)
    # Создаем edge-устройства
    # devices = [
    #     EdgeDevice(device_id=1, task_compute_demand=300, task_data_size=200, task_generation_frequency=0.5),
    #     EdgeDevice(device_id=2, task_compute_demand=400, task_data_size=210, task_generation_frequency=0.3),
    #     EdgeDevice(device_id=3, task_compute_demand=100, task_data_size=200, task_generation_frequency=0.5),
    #     EdgeDevice(device_id=4, task_compute_demand=40, task_data_size=150, task_generation_frequency=0.3),
    #     EdgeDevice(device_id=5, task_compute_demand=500, task_data_size=20, task_generation_frequency=0.5),
    #     EdgeDevice(device_id=6, task_compute_demand=40, task_data_size=300, task_generation_frequency=0.15),
    #     EdgeDevice(device_id=7, task_compute_demand=150, task_data_size=200, task_generation_frequency=0.1),
    #     EdgeDevice(device_id=8, task_compute_demand=200, task_data_size=300, task_generation_frequency=0.1),
    #     EdgeDevice(device_id=9, task_compute_demand=30, task_data_size=20, task_generation_frequency=0.5),
    #     EdgeDevice(device_id=10, task_compute_demand=40, task_data_size=30, task_generation_frequency=0.3)
    # ]

    # ну судя по всему частота генерации задач будет задавать количечством устройств
    # я хз как по другому выкрутить
    # крч кол-во устройств == кол-во задач в секунду
    # секунда это с какой частотой симуляция идет, та пауза в виде sleep(1)
    devices = [
            EdgeDevice(device_id=1, task_compute_demand=500, task_data_size=100, task_generation_frequency=10),
            EdgeDevice(device_id=1, task_compute_demand=200, task_data_size=100, task_generation_frequency=10),
            EdgeDevice(device_id=1, task_compute_demand=200, task_data_size=100, task_generation_frequency=10),
            EdgeDevice(device_id=1, task_compute_demand=200, task_data_size=100, task_generation_frequency=10),
            EdgeDevice(device_id=1, task_compute_demand=200, task_data_size=100, task_generation_frequency=10),
            EdgeDevice(device_id=1, task_compute_demand=400, task_data_size=100, task_generation_frequency=10),
            EdgeDevice(device_id=1, task_compute_demand=100, task_data_size=100, task_generation_frequency=10),
            EdgeDevice(device_id=1, task_compute_demand=100, task_data_size=100, task_generation_frequency=10),
            EdgeDevice(device_id=1, task_compute_demand=100, task_data_size=100, task_generation_frequency=10),
            EdgeDevice(device_id=1, task_compute_demand=100, task_data_size=100, task_generation_frequency=10)
    ]

    # Стартовые метрики
    total_created_tasks = 0
    total_rejected_tasks = 0

    end_time = start_time + simulation_duration

    try:
        while time.time() < end_time:
            current_time = time.time()
            relative_time = current_time - start_time
            prev_relative_time = relative_time - 0.5


            # Логирование состояния нод каждую секунду
            if int(relative_time) - int(prev_relative_time) >= 0.5:
                for node in nodes:
                    prev_relative_time = relative_time
                    node.log_current_state(relative_time)

            for device in devices:
                # Логирование состояния нод каждую секунду
                if int(relative_time) != int(relative_time - 1):

                # if random.random() < device.task_generation_frequency:
                # if current_time >= device.next_task_time:

                    # вообще это некорректно, нужно проверять текущее время и частоты генерации задач устройства
                    task_compute_demand, task_data_size, task_id = device.generate_task()
                    logging.info(f"Edge Device {device.device_id}: Task {task_id} generated. \nParams:\n "
                                 f"task_compute_demand = {task_compute_demand},\n "
                                 f"task_data_size = {task_data_size}")

                    #  отладочная информация для отслеживания параметров нод
                    for node in nodes:
                        logging.info(f"\n---\nNode {node.node_id}\n"
                                     f"flops_load = {node.current_load_flops},\n"
                                     f"network_bytes_load = {node.current_network_load_bytes}\n---")

                    distributor.distribute_task(task_compute_demand, task_data_size, task_id)

                    total_created_tasks += 1

            # Симулируем отключение нод
            for node in nodes:
                threading.Thread(target=node.simulate_failure).start()

            time.sleep(0.25)  # Пауза между итерациями симуляции

    except KeyboardInterrupt:
        logging.info("Simulation stopped by user.")

    # Сохраняем результаты
    total_rejected_tasks = distributor.rejected_tasks
    save_results_to_csv(nodes, total_created_tasks, total_rejected_tasks, simulation_duration)
    calc_tests_results(nodes, total_created_tasks, total_rejected_tasks, simulation_duration)

    save_data_to_csv(nodes)