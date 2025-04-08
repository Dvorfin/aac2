Current working version is:
- visualize_test_2.py - запускать для визуализации
- main.py - запускать для симуляции работы
- edge_device.py - класс эдж устройства
- task_distributor.py - классы алгоритмов
- node.py - класс годы
---
- Round Robin works
- Weighted Round Robin works
- Least Connections works | починил ебать
- Weighted Least Connections отвечаю работает 
---
Последовательность запуска:
main.py - проведение симуляции
vizualize_csv_result.py - для отображаения результат симуляции по взвешенным данным
visualize_test_2.py - более старая версия считает по среднему не совсем корректно

----
save_data_to_csv_all_active_time - сохранение данных в csv просто средних значений
save_weighted_data_to_csv_all_active_time - сохранение данных в csv взвешенных значений за все время теста
save_weighted_data_to_csv - сохранение данных в csv взвешенных значений только за активное время работы серверов