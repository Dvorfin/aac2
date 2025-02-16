import matplotlib.pyplot as plt

def visualize_load_data():
    # Чтение данных из файла
    load_data = {}
    with open("load_data.txt", "r") as f:
        current_node = None
        for line in f:
            if line.startswith("Node"):
                current_node = int(line.split()[1].strip(":"))
                load_data[current_node] = {"time": [], "load": [], "network_load": []}
            else:
                timestamp, load_percentage, network_load_percentage = map(float, line.strip().split(", "))
                load_data[current_node]["time"].append(timestamp)
                load_data[current_node]["load"].append(load_percentage)
                load_data[current_node]["network_load"].append(network_load_percentage)

    # Построение графиков
    plt.figure(figsize=(12, 6))
    for node_id, data in load_data.items():
        plt.plot(data["time"], data["load"], label=f"Node {node_id} Load (%)")

    plt.xlabel("Time (seconds)")
    plt.ylabel("Load (%)")
    plt.title("Compute Node Load Over Time")
    plt.legend()
    plt.grid()
    plt.show()

    # График загрузки сети
    plt.figure(figsize=(12, 6))
    for node_id, data in load_data.items():
        plt.plot(data["time"], data["network_load"], label=f"Node {node_id} Network Load (%)")

    plt.xlabel("Time (seconds)")
    plt.ylabel("Network Load (%)")
    plt.title("Compute Node Network Load Over Time")
    plt.legend()
    plt.grid()
    plt.show()

def visualize_lost_packets():
    # Чтение данных о потерянных пакетах
    time_data = []
    lost_packets_data = []
    total_generated_data = []
    total_processed_data = []
    with open("lost_packets_data.txt", "r") as f:
        next(f)  # Пропускаем заголовок
        for line in f:
            timestamp, lost_packets, total_generated, total_processed = map(float, line.strip().split(", "))
            time_data.append(timestamp)
            lost_packets_data.append(lost_packets)
            total_generated_data.append(total_generated)
            total_processed_data.append(total_processed)

    # Построение графика потерянных пакетов
    plt.figure(figsize=(12, 6))
    plt.plot(time_data, lost_packets_data, label="Lost Packets", color="red")
    plt.plot(time_data, total_generated_data, label="Total Packets Generated", color="blue")
    plt.plot(time_data, total_processed_data, label="Total Packets Processed", color="green")
    plt.xlabel("Time (seconds)")
    plt.ylabel("Packets")
    plt.title("Packet Statistics Over Time")
    plt.legend()
    plt.grid()
    plt.show()

def visualize_active_tasks():
    # Чтение данных о количестве активных задач на нодах
    active_tasks_data = {}
    with open("active_tasks_data.txt", "r") as f:
        current_node = None
        for line in f:
            if line.startswith("Node"):
                current_node = int(line.split()[1].strip(":"))
                active_tasks_data[current_node] = {"time": [], "active_tasks": []}
            else:
                timestamp, active_tasks = map(float, line.strip().split(", "))
                active_tasks_data[current_node]["time"].append(timestamp)
                active_tasks_data[current_node]["active_tasks"].append(active_tasks)

    # Построение графика активных задач
    plt.figure(figsize=(12, 6))
    for node_id, data in active_tasks_data.items():
        plt.plot(data["time"], data["active_tasks"], label=f"Node {node_id} Active Tasks")

    plt.xlabel("Time (seconds)")
    plt.ylabel("Active Tasks")
    plt.title("Active Tasks on Compute Nodes Over Time")
    plt.legend()
    plt.grid()
    plt.show()

if __name__ == "__main__":
    visualize_load_data()
    visualize_lost_packets()
    visualize_active_tasks()