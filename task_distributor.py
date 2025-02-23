import logging


class RoundRobin:
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


class WeightedRoundRobin:
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
