import random
import logging
import threading
import time

class ComputeNode:
    def __init__(self, node_id, compute_power, availability_delay, network_bandwidth, failure_probability, failure_duration):
        self.node_id = node_id
        self.compute_power = compute_power  # в флопсах
        self.availability_delay = availability_delay  # время задержки до начала выполнения задачи
        self.network_bandwidth = network_bandwidth  # пропускная способность для прихода пакетов (в байтах/сек)
        self.failure_probability = failure_probability  # вероятность отключения узла
        self.failure_duration = failure_duration  # время отключения узла
        self.current_load = 0  # текущая загрузка мощности (в флопсах)
        self.network_load = 0  # текущая загрузка сети (в байтах)
        self.lock = threading.Lock()  # Блокировка для потокобезопасности
        self.failure_start_time = None  # Время начала отключения ноды
        self.active_tasks = 0  # Количество активных задач

    def is_available(self):
        # Проверяем, доступен ли узел (не отключен)
        if self.failure_start_time is not None:
            # Если нода отключена, проверяем, истекло ли время отключения
            if time.time() - self.failure_start_time >= self.failure_duration:
                self.failure_start_time = None  # Нода снова доступна
                logging.info(f"Node {self.node_id} is back online.")
            else:
                return False  # Нода все еще отключена

        # Проверяем вероятность отключения
        if random.random() < self.failure_probability:
            self.failure_start_time = time.time()  # Запоминаем время отключения
            logging.info(f"Node {self.node_id} is down for {self.failure_duration} seconds.")
            return False
        return True

    def can_accept_task(self, task_flops, task_size):
        # Проверяем, может ли нода принять задачу с учетом загрузки мощности и сети
        with self.lock:
            has_compute_capacity = (self.current_load + task_flops) <= self.compute_power
            has_network_capacity = (self.network_load + task_size) <= self.network_bandwidth
            return has_compute_capacity and has_network_capacity

    def process_task(self, task_flops, task_size):
        if self.is_available() and self.can_accept_task(task_flops, task_size):
            # Увеличиваем загрузку мощности и сети
            with self.lock:
                self.current_load += task_flops
                self.network_load += task_size
                self.active_tasks += 1

            logging.info(f"Node {self.node_id} started processing a task. "
                         f"Current load: {self.current_load} FLOPs ({self.get_load_percentage()}%), "
                         f"Network load: {self.network_load} bytes ({self.get_network_load_percentage()}%), "
                         f"Active tasks: {self.active_tasks}.")

            # Имитируем выполнение задачи в отдельном потоке
            threading.Thread(target=self._complete_task, args=(task_flops, task_size)).start()

            return True
        else:
            return False

    def _complete_task(self, task_flops, task_size):
        # Имитируем время выполнения задачи
        processing_time = task_flops / self.compute_power + self.availability_delay
        time.sleep(processing_time)

        # Уменьшаем загрузку мощности и сети
        with self.lock:
            self.current_load -= task_flops
            self.network_load -= task_size
            self.active_tasks -= 1

        logging.info(f"Node {self.node_id} completed a task. "
                     f"Current load: {self.current_load} FLOPs ({self.get_load_percentage()}%), "
                     f"Network load: {self.network_load} bytes ({self.get_network_load_percentage()}%), "
                     f"Active tasks: {self.active_tasks}.")

    def get_load_percentage(self):
        # Возвращает загрузку мощности в процентах
        return (self.current_load / self.compute_power) * 100

    def get_network_load_percentage(self):
        # Возвращает загрузку сети в процентах
        return (self.network_load / self.network_bandwidth) * 100

    def reset_load(self):
        # Сбрасываем загрузку (для симуляции)
        self.current_load = 0
        self.network_load = 0
        self.active_tasks = 0

class EdgeDevice:
    def __init__(self, device_id, availability_delay, required_power, task_size, task_generation_rate):
        self.device_id = device_id
        self.availability_delay = availability_delay  # время задержки до начала выполнения задачи
        self.required_power = required_power  # требуемая мощность на обработку пакета в флопсах
        self.task_size = task_size  # объем задачи в байтах
        self.task_generation_rate = task_generation_rate  # частота генерации задач

    def generate_task(self):
        # Генерация задачи с требуемой мощностью и объемом
        return self.required_power, self.task_size


import time
import logging
from collections import defaultdict

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[
        logging.FileHandler("../simulation.log"),  # Логирование в файл
        logging.StreamHandler()  # Логирование в консоль
    ])

def simulate_round_robin(edge_devices, compute_nodes, simulation_time):
    current_node_index = 0
    start_time = time.time()
    load_data = defaultdict(list)  # Словарь для хранения данных о нагрузке нод
    lost_packets_data = []  # Список для хранения данных о потерянных пакетах
    lost_packets_count = 0  # Счетчик потерянных пакетов
    total_packets_generated = 0  # Общее количество сгенерированных пакетов
    total_packets_processed = 0  # Общее количество успешно обработанных пакетов
    active_tasks_data = defaultdict(list)  # Словарь для хранения данных о количестве активных задач на нодах

    while time.time() - start_time < simulation_time:
        for edge_device in edge_devices:
            task_flops, task_size = edge_device.generate_task()
            node = compute_nodes[current_node_index]

            total_packets_generated += 1
            logging.info(f"Edge Device {edge_device.device_id} generated a task requiring {task_flops} FLOPs and {task_size} bytes.")
            task_accepted = node.process_task(task_flops, task_size)

            if task_accepted:
                total_packets_processed += 1
                logging.info(f"Task assigned to Node {node.node_id}.")
            else:
                # Проверяем все ноды на возможность принять задачу
                task_assigned = False
                for node in compute_nodes:
                    if node.process_task(task_flops, task_size):
                        task_assigned = True
                        total_packets_processed += 1
                        break

                if not task_assigned:
                    lost_packets_count += 1
                    logging.info(f"Task from Edge Device {edge_device.device_id} was lost. Total lost packets: {lost_packets_count}.")

            # Записываем данные о нагрузке
            for node in compute_nodes:
                load_data[node.node_id].append((time.time() - start_time, node.get_load_percentage(), node.get_network_load_percentage()))

            # Записываем данные о количестве активных задач на нодах
            for node in compute_nodes:
                active_tasks_data[node.node_id].append((time.time() - start_time, node.active_tasks))

            # Записываем данные о потерянных пакетах
            lost_packets_data.append((time.time() - start_time, lost_packets_count, total_packets_generated, total_packets_processed))

            # Переходим к следующей ноде
            current_node_index = (current_node_index + 1) % len(compute_nodes)
            time.sleep(1 / edge_device.task_generation_rate)  # Задержка между генерацией задач

    return load_data, lost_packets_data, active_tasks_data


if __name__ == "__main__":
    setup_logging()

    # Создаем 5 вычислительных нод
    compute_nodes = [
        ComputeNode(node_id=1, compute_power=1e9, availability_delay=0.1, network_bandwidth=1e6, failure_probability=0.05, failure_duration=10),
        ComputeNode(node_id=2, compute_power=2e9, availability_delay=0.2, network_bandwidth=2e6, failure_probability=0.1, failure_duration=5)
    ]

    # Создаем несколько edge-устройств
    edge_devices = [
        EdgeDevice(device_id=1, availability_delay=0.05, required_power=1e2, task_size=1e1, task_generation_rate=2),
        EdgeDevice(device_id=2, availability_delay=0.1, required_power=2e2, task_size=2e2, task_generation_rate=1),
        EdgeDevice(device_id=3, availability_delay=0.15, required_power=1.5e3, task_size=1.5e2, task_generation_rate=1.5),
        EdgeDevice(device_id=1, availability_delay=0.05, required_power=1e2, task_size=1e5, task_generation_rate=2),
        EdgeDevice(device_id=2, availability_delay=0.1, required_power=2e8, task_size=2e5, task_generation_rate=1),
        EdgeDevice(device_id=3, availability_delay=0.15, required_power=1.5e8, task_size=1.5e5, task_generation_rate=1.5)
    ]

    # Запускаем симуляцию на 30 секунд
    load_data, lost_packets_data, active_tasks_data = simulate_round_robin(edge_devices, compute_nodes, simulation_time=30)

    # Сохраняем данные для визуализации
    with open("load_data.txt", "w") as f:
        for node_id, data in load_data.items():
            f.write(f"Node {node_id}:\n")
            for timestamp, load_percentage, network_load_percentage in data:
                f.write(f"{timestamp}, {load_percentage}, {network_load_percentage}\n")

    # Сохраняем данные о потерянных пакетах
    with open("lost_packets_data.txt", "w") as f:
        f.write("Time, Lost Packets, Total Packets Generated, Total Packets Processed\n")
        for timestamp, lost_packets, total_generated, total_processed in lost_packets_data:
            f.write(f"{timestamp}, {lost_packets}, {total_generated}, {total_processed}\n")

    # Сохраняем данные о количестве активных задач на нодах
    with open("active_tasks_data.txt", "w") as f:
        for node_id, data in active_tasks_data.items():
            f.write(f"Node {node_id}:\n")
            for timestamp, active_tasks in data:
                f.write(f"{timestamp}, {active_tasks}\n")


