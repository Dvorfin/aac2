import threading
import time
import random
import queue
import logging

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
        :param weight: вес узла.
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