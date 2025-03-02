import threading
import queue
import time
import random
import logging
import csv

from node import Node
from task_distributor import RoundRobin, WeightedRoundRobin, LeastConnection
from edge_device import EdgeDevice

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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

    logging.info(f"Simulation results saved to {filename}")



if __name__ == "__main__":
    # Создаем ноды
    nodes = [
        Node(node_id=1, compute_power_flops=1000, delay_seconds=2, bandwidth_bytes=500, failure_probability=0.1,
             downtime_seconds=5),
        Node(node_id=2, compute_power_flops=2000, delay_seconds=1, bandwidth_bytes=1000, failure_probability=0.2,
             downtime_seconds=3),
        Node(node_id=3, compute_power_flops=500, delay_seconds=3, bandwidth_bytes=750, failure_probability=0.15,
             downtime_seconds=8)
    ]

    # Устанавливаем start_time для всех нод
    start_time = time.time()
    for node in nodes:
        node.start_time = start_time

    # Создаем дистрибьютор задач
    distributor = LeastConnection(nodes)

    # Создаем edge-устройства
    devices = [
        EdgeDevice(device_id=1, task_compute_demand=300, task_data_size=200, task_generation_frequency=0.5),
        EdgeDevice(device_id=2, task_compute_demand=400, task_data_size=210, task_generation_frequency=0.3),
        EdgeDevice(device_id=3, task_compute_demand=100, task_data_size=200, task_generation_frequency=0.5),
        EdgeDevice(device_id=4, task_compute_demand=40, task_data_size=150, task_generation_frequency=0.3),
        EdgeDevice(device_id=5, task_compute_demand=500, task_data_size=20, task_generation_frequency=0.5),
        EdgeDevice(device_id=6, task_compute_demand=40, task_data_size=300, task_generation_frequency=0.15),
        EdgeDevice(device_id=7, task_compute_demand=150, task_data_size=200, task_generation_frequency=0.1),
        EdgeDevice(device_id=8, task_compute_demand=200, task_data_size=300, task_generation_frequency=0.1),
        EdgeDevice(device_id=9, task_compute_demand=30, task_data_size=20, task_generation_frequency=0.5),
        EdgeDevice(device_id=10, task_compute_demand=40, task_data_size=30, task_generation_frequency=0.3)
    ]

    # Стартовые метрики
    total_created_tasks = 0
    total_rejected_tasks = 0
    simulation_duration = 30  # Длительность симуляции в секундах
    end_time = start_time + simulation_duration

    try:
        while time.time() < end_time:
            current_time = time.time()
            relative_time = current_time - start_time

            # Логирование состояния нод каждую секунду
            if int(relative_time) != int(relative_time - 1):
                for node in nodes:
                    node.log_current_state(relative_time)

            for device in devices:
                if random.random() < device.task_generation_frequency:
                    # вообще это некорректно, нужно проверять текущее время и частоты генерации задач устройства
                    task_compute_demand, task_data_size, task_id = device.generate_task()
                    logging.info(f"Edge Device {device.device_id}: Task {task_id} generated.")
                    distributor.distribute_task(task_compute_demand, task_data_size, task_id)
                    total_created_tasks += 1

            # Симулируем отключение нод
            for node in nodes:
                threading.Thread(target=node.simulate_failure).start()

            time.sleep(1)  # Пауза между итерациями симуляции

    except KeyboardInterrupt:
        logging.info("Simulation stopped by user.")

    # Сохраняем результаты
    total_rejected_tasks = distributor.rejected_tasks
    save_results_to_csv(nodes, total_created_tasks, total_rejected_tasks, simulation_duration)