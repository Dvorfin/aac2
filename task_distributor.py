import logging


class WeightedRoundRobin:
    def __init__(self, nodes: list):
        """
        Класс для распределения задач между нодами по алгоритму Round Robin.

        :param nodes: Список нод.
        """
        self.nodes = nodes
        self.current_node_index = 0
        self.rejected_tasks = 0  # Счетчик отклоненных задач
        self.nodes_weights = [0.0] * len(nodes)
        self.normalized_nodes_weights = [0.0] * len(nodes)

    def calc_node_weights(self, nodes):
        '''Вычисляем вес нод'''
        for i in range(len(nodes)):
            flops, delay, bandwidth, fp = (nodes[i].compute_power_flops,
                                           nodes[i].delay_seconds,
                                           nodes[i].bandwidth_bytes,
                                           nodes[i].failure_probability)
            w = (flops + bandwidth) / (delay * 1000 + fp)
            self.nodes_weights[i] = w

    def normalize_node_weights(self):
        '''Номрализуем веса нод в диапазоне от 1 до 10.
        Чем больше вес, тем лучше нода'''

        # Перещитываем веса
        self.calc_node_weights(self.nodes)

        max_weight = max(self.nodes_weights)
        min_weight = min(self.nodes_weights)
        for i in range(len(self.nodes_weights)):
            normalized_weight = 1 + 9 * ((self.nodes_weights[i] - min_weight) / (max_weight - min_weight))
            self.normalized_nodes_weights[i] = normalized_weight

    def distribute_task(self, task_compute_demand: float, task_data_size: float, task_id: str):
        """
        Распределяет задачу между нодами по алгоритму Round Robin.

        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        :param task_id: Идентификатор задачи.
        """

        # перещитываем веса нод
        self.normalize_node_weights()

        for i in range(len(self.nodes)):
            '''Проверяем доступна ли нода и может ли принять задачу,
            после этого смотрим на веса оставшихся нод и выбираем с наивысшим весом
            Недоступным нодам делаем вес равный нулю'''
            if self.nodes[i].is_available() and self.nodes[i].can_accept_task(task_compute_demand, task_data_size):
                continue
            else:
                self.normalized_nodes_weights[i] = 0

        while True:
            # Если все ноды отключены то пропускаем
            if sum(self.normalized_nodes_weights) == 0:
                logging.error(f"No available nodes to assign task {task_id}. Skipping...")
                self.rejected_tasks += 1
                break

            # определяем максимальный доступный вес оставшихся нод
            max_available_weights = max(self.normalized_nodes_weights)

            # определяем индекс ноды с максимальным весом
            node_index = self.normalized_nodes_weights.index(max_available_weights)
            # отдаем задачу
            self.nodes[node_index].add_task(task_compute_demand, task_data_size, task_id)

            # перещитываем веса нод
            self.normalize_node_weights()
            logging.error(f"Current weights of Nodes {self.normalized_nodes_weights}")
            break


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


class LeastConnection:
    def __init__(self, nodes: list):
        """Класс для распределения задач между нодами по алгоритму Least Connection.
            :param nodes: Список нод.
        """
        self.nodes = nodes
        self.current_node_index = 0
        self.nodes_connections = [0] * len(nodes)
        self.rejected_tasks = 0  # Счетчик отклоненных задач

    def updated_nodes_connections(self, nodes):
        for i in range(len(nodes)):
            self.nodes_connections[i] = nodes[i].get_current_tasks_on_node()  # записываем сколько задач на каждой из нод

    def distribute_task(self, task_compute_demand: float, task_data_size: float, task_id: str):
        """ Распределяет задачу между нодами по алгоритму Least Connections.
        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        :param task_id: Идентификатор задачи. """
        self.updated_nodes_connections(self.nodes)  # обновляем количество подключений перед распределением задач

        for i in range(len(self.nodes)):
            '''Проверяем доступна ли нода и может ли принять задачу,
            после этого смотрим на количество подключений (задач  на ноде) и
            выбираем с минимальным значением'''
            if self.nodes[i].is_available() and self.nodes[i].can_accept_task(task_compute_demand, task_data_size):
                continue
            else:
                self.nodes_connections[i] = -1  # если нода недоступна, то ставим ей отрицательное кол-во подключений

        while True:
            if all(conn == -1 for conn in self.nodes_connections):  # если все ноды заняты или не могут взять задачу
                logging.error(f"No available nodes to assign task {task_id}. Skipping...")
                self.rejected_tasks += 1
                break

            min_connections = min(self.nodes_connections)   # определяем минимальное кол-во подключений среди доступных нод
            min_connections_node_index = self.nodes_connections.index(min_connections)  # определяем первый индекс среди доступных нод

            # отдаем задачу
            self.nodes[min_connections_node_index].add_task(task_compute_demand, task_data_size, task_id)

            # обновляем количество подключений
            self.updated_nodes_connections(self.nodes)
            logging.error(f"Current connections of Nodes {self.nodes_connections}")
            break


class WeightedLeastConnection:
    def __init__(self, nodes: list):
        """Класс для распределения задач между нодами по алгоритму Weighted Least Connection.
            :param nodes: Список нод.
        """
        self.nodes = nodes
        self.current_node_index = 0
        self.nodes_connections = [0] * len(nodes)
        self.rejected_tasks = 0  # Счетчик отклоненных задач

        self.nodes_weights = [0.0] * len(nodes)
        self.normalized_nodes_weights = self.normalize_node_weights()   # один раз вычисляем вес нод
        self.wlc_weight = [0.0] * len(nodes)

    def calc_node_weights(self, nodes):
        '''Вычисляем вес нод'''
        for i in range(len(nodes)):
            flops, delay, bandwidth, fp = (nodes[i].compute_power_flops,
                                           nodes[i].delay_seconds,
                                           nodes[i].bandwidth_bytes,
                                           nodes[i].failure_probability)
            w = (flops + bandwidth) / (delay * 1000 + fp)
            self.nodes_weights[i] = w

    def normalize_node_weights(self):
        '''Номрализуем веса нод в диапазоне от 1 до 10.
        Чем больше вес, тем лучше нода'''

        normalized_nodes_weights = []

        # Перещитываем веса
        self.calc_node_weights(self.nodes)

        max_weight = max(self.nodes_weights)
        min_weight = min(self.nodes_weights)

        for i in range(len(self.nodes_weights)):
            normalized_weight = 1 + 9 * ((self.nodes_weights[i] - min_weight) / (max_weight - min_weight))
            normalized_nodes_weights.append(normalized_weight)

        return normalized_nodes_weights

    def updated_nodes_connections(self, nodes):
        for i in range(len(nodes)):
            self.nodes_connections[i] = nodes[i].get_current_tasks_on_node()  # записываем сколько задач на каждой из нод

    def calc_wlc_node_weights(self, nodes):
        '''Вычисляем вес нод для Weighted Least Connections
        чем меньше значение, тем лучше
        w = active_connections/normalize_node_weight'''

        # обновляем кол-во подключений, чтобы далее вызывать только функцию calc_wlc_node_weights
        self.updated_nodes_connections(nodes)

        for i in range(len(self.nodes)):
            # вычисляем вес по формуле  w = Vc/normalize_node_weight
            self.wlc_weight[i] = self.nodes_connections[i]/self.normalized_nodes_weights[i]

    def distribute_task(self, task_compute_demand: float, task_data_size: float, task_id: str):
        """Распределяет задачу между нодами по алгоритму Weighted Least Connections.
        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        :param task_id: Идентификатор задачи.
        """

        # обновляем вес нод
        self.calc_wlc_node_weights(self.nodes)

        for i in range(len(self.nodes)):
            '''Проверяем доступна ли нода и может ли принять задачу,
            после этого смотрим на количество подключений (задач  на ноде) и
            выбираем с минимальным значением'''
            if self.nodes[i].is_available() and self.nodes[i].can_accept_task(task_compute_demand, task_data_size):
                continue
            else:
                self.wlc_weight[i] = 5000 # если нода недоступна, то ставим ей большой вес

        while True:
            if all(conn == 5000 for conn in self.wlc_weight):  # если все ноды заняты или не могут взять задачу
                logging.error(f"No available nodes to assign task {task_id}. Skipping...")
                self.rejected_tasks += 1
                break

            min_available_weights = min(self.wlc_weight)   # определяем минимальный вес среди доступных нод
            min_weight_node_index = self.wlc_weight.index(min_available_weights)  # определяем первый индекс среди доступных нод

            # отдаем задачу
            self.nodes[min_weight_node_index].add_task(task_compute_demand, task_data_size, task_id)

            logging.error(f"Current WLC weights of Nodes {self.wlc_weight} | Task distributed to Node id = {min_weight_node_index + 1}")
            logging.error(f"Current WLC Nodes params {self.wlc_weight} | node index = {min_weight_node_index}")
            # обновляем вес нод
            self.calc_wlc_node_weights(self.nodes)
            break