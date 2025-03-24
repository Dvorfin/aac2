import time

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
        self.next_task_time = self.calculate_next_task_time()
        self.task_id_counter = 0

    def calculate_next_task_time(self):
        return time.time() + 1 / self.task_generation_frequency

    def generate_task(self) -> tuple:
        """
        Генерирует новую задачу.

        :return: Tuple с параметрами задачи (compute_demand, data_size, task_id).
        """
        self.task_id_counter += 1
        # Обновляем время следующей генерации задачи
        self.next_task_time = self.calculate_next_task_time()
        task_id = f"D{self.device_id}_T{int(time.time())}"
        return self.task_compute_demand, self.task_data_size, task_id
