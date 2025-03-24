import logging


class WeightedRoundRobin:
    def __init__(self, nodes: list):
        """
        Класс для распределения задач между нодами по алгоритму Weighted Round Robin.

        :param nodes: Список нод. Каждая нода должна иметь атрибут `weight`, определяющий ее вес.
        """
        self.nodes = nodes
        self.current_node_index = 0
        self.rejected_tasks = 0  # Счетчик отклоненных задач
        self.task_counters = [0] * len(nodes)  # Счетчик задач для каждой ноды

    def distribute_task(self, task_compute_demand: float, task_data_size: float, task_id: str):
        """
        Распределяет задачу между нодами по алгоритму Weighted Round Robin.

        :param task_compute_demand: Требуемая мощность задачи (FLOPS).
        :param task_data_size: Объем данных задачи (байты).
        :param task_id: Идентификатор задачи.
        """
        n = len(self.nodes)
        start_index = self.current_node_index

        while True:
            node = self.nodes[self.current_node_index]
            weight = node.weight

            # Проверяем, можно ли назначить задачу на текущую ноду
            if (
                node.is_available()
                and node.can_accept_task(task_compute_demand, task_data_size)
                and self.task_counters[self.current_node_index] < weight
            ):
                node.add_task(task_compute_demand, task_data_size, task_id)
                self.task_counters[self.current_node_index] += 1
                break
            else:
                logging.warning(f"Node {node.node_id}: Unable to accept task {task_id}. Trying next node...")

            # Переходим к следующей ноде
            self.current_node_index = (self.current_node_index + 1) % n

            # Если прошли полный круг и не нашли подходящую ноду
            if self.current_node_index == start_index:
                logging.error(f"No available nodes to assign task {task_id}. Skipping...")
                self.rejected_tasks += 1
                break

        # Сбрасываем счетчики, если все ноды использовали свои веса
        if all(counter >= node.weight for counter, node in zip(self.task_counters, self.nodes)):
            self.task_counters = [0] * len(self.nodes)



class Node:
    def __init__(self, node_id, weight, compute_capacity, data_capacity):
        self.node_id = node_id
        self.weight = weight
        self.compute_capacity = compute_capacity
        self.data_capacity = data_capacity
        self.current_compute = 0
        self.current_data = 0

    def is_available(self):
        return self.current_compute < self.compute_capacity and self.current_data < self.data_capacity

    def can_accept_task(self, task_compute_demand, task_data_size):
        return (
            self.current_compute + task_compute_demand <= self.compute_capacity
            and self.current_data + task_data_size <= self.data_capacity
        )

    def add_task(self, task_compute_demand, task_data_size, task_id):
        self.current_compute += task_compute_demand
        self.current_data += task_data_size
        print(f"Task {task_id} assigned to Node {self.node_id}")



def test_1():
    # Создаем ноды с разными весами
    nodes = [
        Node(node_id="Node1", weight=3, compute_capacity=100, data_capacity=1000),
        Node(node_id="Node2", weight=2, compute_capacity=100, data_capacity=1000),
        Node(node_id="Node3", weight=1, compute_capacity=100, data_capacity=1000),
    ]

    # Создаем балансировщик
    balancer = WeightedRoundRobin(nodes)

    # Распределяем задачи
    tasks = [
        {"task_id": "Task1", "compute_demand": 10, "data_size": 100},
        {"task_id": "Task2", "compute_demand": 20, "data_size": 200},
        {"task_id": "Task3", "compute_demand": 30, "data_size": 300},
        {"task_id": "Task4", "compute_demand": 40, "data_size": 400},
        {"task_id": "Task5", "compute_demand": 50, "data_size": 500},
        {"task_id": "Task6", "compute_demand": 60, "data_size": 600},
    ]

    for task in tasks:
        balancer.distribute_task(task["compute_demand"], task["data_size"], task["task_id"])
        print(balancer.task_counters)


    nodes = [(1000, 2, 500, 0.1),
            (2000, 1, 1000, 0.2),
            (500, 3, 750, 0.15)]


    weights = []
    for n in nodes:
        flops, delay, bandwidth, fp = n[0], n[1], n[2], n[3]

        w = (flops + bandwidth) / (delay * 1000 + fp)
        weights.append(w)

        print(f"node weight: {w}")

    max_w = max(weights)
    min_w = min(weights)

    for w in weights:
        normalized_weight = 1 + 9 * ((w - min_w) / (max_w - min_w))
        print(f"Normalized weight: {normalized_weight}")


    test = [0, 2, 0, 0, 2, 1, 1]

    max_weight = max(test)
    node_index = test.index(max_weight)
    print(node_index, test[node_index])


if __name__ == "__main__":
    data = [
        (0.4026627540588379, 3),
        (0.40422821044921875, 2),
        (0.4052727222442627, 1),
        (0.4073457717895508, 0),
        (1.0075187683105469, 0),
        (1.4101488590240479, 3),
        (1.4116675853729248, 2),
        (1.413193941116333, 1),
        (1.414201021194458, 0),
        (2.0151002407073975, 0),
        (2.4175548553466797, 3),
        (2.419574499130249, 2),
        (2.421321153640747, 1),
        (2.422846794128418, 0),
        (3.0233957767486572, 0),
        (3.4256250858306885, 3),
        (3.4271399974823, 2),
        (3.42814564704895, 1),
        (3.4298295974731445, 4),
        (4.029853820800781, 5),
        (5.035712957382202, 4),
        (6.041590929031372, 9),
        (7.047309637069702, 8),
        (8.053276300430298, 0),
        (8.459699869155884, 1),
        (9.059858560562134, 0),
        (9.46295166015625, 3),
        (9.4639573097229, 2),
        (9.4644615650177, 1),
        (9.465975999832153, 0)
    ]

    total_time = data[-1][0]  # Общее время наблюдения
    weighted_sum = 0.0

    for i in range(1, len(data)):
        prev_time, prev_tasks = data[i - 1]
        curr_time, curr_tasks = data[i]
        delta = curr_time - prev_time
        weighted_sum += prev_tasks * delta  # Количество задач не менялось до текущего замера

    mat_expectation = weighted_sum / total_time

    print(f"Математическое ожидание количества задач на ноде: {mat_expectation:.4f}")