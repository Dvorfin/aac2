import simpy
import matplotlib.pyplot as plt
import random
# Количество хостов
NUM_HOSTS = 10
# Количество задач
NUM_TASKS = 200000
# Время выполнения одной задачи на самом медленном хосте
BASE_TASK_DURATION = 10
# Время между поступлением задач
TASK_INTERVAL = 3
# Общее время симуляции
SIMULATION_TIME = 200


def task(env, name, host, host_speed, host_loads, task_load):
    """
    Задача, которая выполняется на хосте.
    """
    start_time = env.now
    print(f"Task {name} started on {host} (speed: {host_speed}x) at time {start_time}")

    # Время выполнения задачи зависит от скорости хоста
    task_duration = BASE_TASK_DURATION / host_speed
    yield env.timeout(task_duration)  # Имитация выполнения задачи

    finish_time = env.now
    print(f"Task {name} finished on {host} at time {finish_time}. Execution time: {task_duration:.2f} units")

    # Обновляем загрузку хоста
    for t in range(int(start_time), int(finish_time)):
        if t < len(host_loads[host]):
            host_loads[host][t] += task_load  # Увеличиваем загрузку на task_load для каждой секунды


def task_generator(env, hosts, host_speeds, host_loads):
    """
    Генератор задач, который распределяет задачи по хостам с использованием Round Robin.
    """
    for i in range(NUM_TASKS):
        # Выбор хоста по Round Robin
        host_index = i % NUM_HOSTS
        host = hosts[host_index]
        host_speed = host_speeds[host_index]

        # Загрузка задачи (случайное значение от 10% до 50%)
        task_load = random.uniform(0.1, 0.5)

        # Создание задачи
        env.process(task(env, f"Task-{i}", host, host_speed, host_loads, task_load))

        # Пауза перед следующей задачей
        yield env.timeout(TASK_INTERVAL)


def main():
    # Создаем окружение SimPy
    env = simpy.Environment()

    # Создаем список хостов и их скоростей
    hosts = [f"Host-{i}" for i in range(NUM_HOSTS)]
    host_speeds = [1.0, 1.5, 2.0, 0.5, 1.0]  # Скорость выполнения задач для каждого хоста

    # Создаем словарь для хранения загрузки хостов
    host_loads = {host: [0] * SIMULATION_TIME for host in hosts}

    # Запускаем генератор задач
    env.process(task_generator(env, hosts, host_speeds, host_loads))

    # Запускаем симуляцию
    env.run(until=SIMULATION_TIME)

    # Выводим загрузку хостов в процентах
    for host in hosts:
        print(f"{host} load over time:")
        for t in range(SIMULATION_TIME):
            # Загрузка в процентах (нормализуем до 100%)
            load_percentage = min(host_loads[host][t], 1) * 100
            print(f"  Time {t}: {load_percentage:.2f}%")

    # Визуализация загрузки хостов
    plt.figure(figsize=(10, 6))
    for host in hosts:
        load_percentages = [min(host_loads[host][t], 1) * 100 for t in range(SIMULATION_TIME)]
        plt.plot(range(SIMULATION_TIME), load_percentages, label=host)

    plt.xlabel("Time (seconds)")
    plt.ylabel("Load (%)")
    plt.title("Host Load Over Time")
    plt.legend()
    plt.grid()
    plt.show()


if __name__ == "__main__":
    main()