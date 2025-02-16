import threading
import queue
import time
import random
import logging
import csv

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Node:
    def __init__(self, node_id: int, compute_power_flops: float, delay_seconds: float,
                 bandwidth_bytes: float, failure_probability: float,
                 downtime_seconds: float):
        """
        Класс вычислительной ноды.

        :param node_id: Номер ноды.
        :param compute_power_flops: Мощность ноды в FLOPS.
        :param delay_seconds: Время задержки до начала выполнения задачи (в секундах).
        :param bandwidth_bytes: Пропускная способность канала для задачи (в байтах/сек).
        :param failure_probability: Вероятность отключения ноды (0.0 - 1.0).
        :param downtime_seconds: Время отключения ноды (в секундах).
        """
        self.node_id = node_id
        self.compute_power_flops = compute_power_flops
        self.delay_seconds = delay_seconds
        self.bandwidth_bytes = bandwidth_bytes
        self.failure_probability = failure_probability
        self.downtime_seconds = downtime_seconds
        self.current_load_flops = 0.0
        self.current_network_load_bytes = 0.0
        self.running_tasks_count = 0
        self.is_down = False
        self.lock = threading.Lock()
        self.task_queue = queue.Queue()

        # Для сбора статистики
        self.load_history = []  # [(relative_time, load_flops)]
        self.network_load_history = []  # [(relative_time, network_load_bytes)]
        self.running_tasks_history = []  # [(relative_time, running_tasks_count)]

    def is_available(self) -> bool:
        """
        Проверяет, доступна ли нода для выполнения задач.
        """
        return not self.is_down

    def can_accept_task(self, task_compute_demand: float, task_data_size: float) -> bool:
        """
        Проверяет, может ли нода принять новую задачу.

        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        :return: True, если нода может принять задачу, иначе False.
        """
        with self.lock:
            return (self.current_load_flops + task_compute_demand <= self.compute_power_flops and
                    self.current_network_load_bytes + task_data_size <= self.bandwidth_bytes)

    def add_task(self, task_compute_demand: float, task_data_size: float, task_id: str):
        """
        Добавляет задачу в очередь для выполнения.

        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        :param task_id: Идентификатор задачи.
        """
        if self.can_accept_task(task_compute_demand, task_data_size):
            with self.lock:
                self.current_load_flops += task_compute_demand
                self.current_network_load_bytes += task_data_size
                self.running_tasks_count += 1
            self.task_queue.put((task_compute_demand, task_data_size, task_id))
            self._start_processing()
            logging.info(f"Node {self.node_id}: Task {task_id} added.")
        else:
            logging.warning(f"Node {self.node_id}: Cannot accept task {task_id}. Not enough resources.")

    def _process_task(self, task_compute_demand: float, task_data_size: float, task_id: str):
        """
        Обрабатывает одну задачу в отдельном потоке.

        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        :param task_id: Идентификатор задачи.
        """
        # Симуляция времени передачи данных
        data_transfer_time = task_data_size / self.bandwidth_bytes
        logging.info(f"Node {self.node_id}: Task {task_id} data transfer started (size={task_data_size} bytes).")
        time.sleep(data_transfer_time)
        logging.info(f"Node {self.node_id}: Task {task_id} data transfer completed.")

        # Симуляция задержки до начала выполнения задачи
        time.sleep(self.delay_seconds)
        logging.info(f"Node {self.node_id}: Task {task_id} execution delay completed.")

        # Симуляция выполнения задачи
        execution_time = task_compute_demand / self.compute_power_flops
        logging.info(
            f"Node {self.node_id}: Task {task_id} execution started (compute demand={task_compute_demand} FLOPS).")
        time.sleep(execution_time)
        logging.info(f"Node {self.node_id}: Task {task_id} execution completed.")

        # Освобождение ресурсов
        with self.lock:
            self.current_load_flops -= task_compute_demand
            self.current_network_load_bytes -= task_data_size
            self.running_tasks_count -= 1

        # Сохраняем текущую загрузку после завершения задачи
        self._log_metrics()

    def _start_processing(self):
        """
        Запускает обработку задач из очереди.
        """
        while not self.task_queue.empty():
            task = self.task_queue.get()
            thread = threading.Thread(target=self._process_task, args=task)
            thread.start()

    def simulate_failure(self):
        """
        Симулирует отключение ноды.
        """
        if random.random() < self.failure_probability:
            self.is_down = True
            logging.warning(f"Node {self.node_id}: Node failed. Downtime starts for {self.downtime_seconds} seconds.")
            time.sleep(self.downtime_seconds)
            self.is_down = False
            logging.info(f"Node {self.node_id}: Node recovered after downtime.")

    def _log_metrics(self):
        """
        Сохраняет текущие метрики загрузки ноды.
        """
        relative_time = time.time() - self.start_time
        with self.lock:


            load_in_percent = (self.current_load_flops / self.compute_power_flops) * 100
            # self.load_history.append((relative_time, self.current_load_flops))
            self.load_history.append((relative_time, load_in_percent))

            load_network_in_percent = (self.current_network_load_bytes / self.bandwidth_bytes) * 100
            # self.network_load_history.append((relative_time, self.current_network_load_bytes))
            self.network_load_history.append((relative_time, load_network_in_percent))


            self.running_tasks_history.append((relative_time, self.running_tasks_count))

    def log_current_state(self, relative_time: float):
        """
        Сохраняет текущее состояние ноды в определенный момент времени.

        :param relative_time: Относительное время с начала симуляции.
        """
        with self.lock:
            load_in_percent = (self.current_load_flops / self.compute_power_flops) * 100
            #self.load_history.append((relative_time, self.current_load_flops))
            self.load_history.append((relative_time, load_in_percent))

            load_network_in_percent = (self.current_network_load_bytes / self.bandwidth_bytes) * 100
            #self.network_load_history.append((relative_time, self.current_network_load_bytes))
            self.network_load_history.append((relative_time, load_network_in_percent))


            self.running_tasks_history.append((relative_time, self.running_tasks_count))


class EdgeDevice:
    def __init__(self, device_id: int, task_compute_demand: float, task_data_size: float,
                 task_generation_frequency: float):
        """
        Класс edge-устройства.

        :param device_id: Номер устройства.
        :param task_compute_demand: Требуемая мощность генерируемой задачи (FLOPS).
        :param task_data_size: Объем задачи в байтах для передачи по пропускному каналу.
        :param task_generation_frequency: Частота генерации задач (задач/сек).
        """
        self.device_id = device_id
        self.task_compute_demand = task_compute_demand
        self.task_data_size = task_data_size
        self.task_generation_frequency = task_generation_frequency

    def generate_task(self) -> tuple:
        """
        Генерирует новую задачу.

        :return: Tuple с параметрами задачи (compute_demand, data_size, task_id).
        """
        task_id = f"D{self.device_id}_T{int(time.time())}"
        return self.task_compute_demand, self.task_data_size, task_id


class TaskDistributor:
    def __init__(self, nodes: list):
        """
        Класс для распределения задач между нодами по алгоритму Round Robin.

        :param nodes: Список нод.
        """
        self.nodes = nodes
        self.current_node_index = 0
        self.rejected_tasks = 0  # Счетчик отклоненных задач

    def distribute_task(self, task_compute_demand: float, task_data_size: float, task_id: str):
        """
        Распределяет задачу между нодами по алгоритму Round Robin.

        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        :param task_id: Идентификатор задачи.
        """
        n = len(self.nodes)
        start_index = self.current_node_index

        while True:
            node = self.nodes[self.current_node_index]
            if node.is_available() and node.can_accept_task(task_compute_demand, task_data_size):
                node.add_task(task_compute_demand, task_data_size, task_id)
                break
            else:
                logging.warning(f"Node {node.node_id}: Unable to accept task {task_id}. Trying next node...")

            self.current_node_index = (self.current_node_index + 1) % n
            if self.current_node_index == start_index:
                logging.error(f"No available nodes to assign task {task_id}. Skipping...")
                self.rejected_tasks += 1
                break


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


# Пример использования
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
    distributor = TaskDistributor(nodes)

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
    simulation_duration = 120  # Длительность симуляции в секундах
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