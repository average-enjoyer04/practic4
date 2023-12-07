from flask import Flask, request, render_template
import json
import socket
app = Flask(__name__)

class ReportGenerator:  # для генерации отчета
    def __init__(self):
        self.report = []

    def generate_report(self, split_data):
        source_ip = split_data[0]
        url = split_data[1] + " (" + split_data[2] + ")"
        time = split_data[3]
        # Поиск записи с указанным URL и SourceIP
        for record in self.report:
            if record["URL"] == url and record["SourceIP"] == source_ip and record["TimeInterval"] == time:
                # Увеличение счетчика Count
                record["Count"] += 1
                return

        # Создание новой записи
        record = {
            "Id": len(self.report) + 1,
            "Pid": None,
            "URL": url,
            "SourceIP": source_ip,
            "TimeInterval": time,
            "Count": 1
        }

        # Добавление записи в отчет
        self.report.append(record)


class DatabaseConnection:
    def send_message(self, message):  # обращаемся к базе данных
        message = "QPUSH" + " " + message
        host = '127.0.0.1'
        port = 6379
        # Создание TCP сокета и установка соединения
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.sendall(message.encode())  # отправка запроса
        data = sock.recv(1024)  # приём ответа
        sock.close()  # закрытие сокета
        # print(data)

    def receive_message(self):
        host = '127.0.0.1'
        port = 6379
        message = "QPOP"
        # Создание TCP сокета и установка соединения
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.sendall(message.encode())  # отправка запроса
        report_generator = ReportGenerator()
        while True:
            data = sock.recv(1024).decode('utf-8')
            if "Queue is empty" == data:  # Проверка на наличие строки "Queue is empty"
                print("Queue is empty")
                break
            # Извлечение полей URL и SourceIP из данных
            split_data = data.split()

            # Генерация отчета
            report_generator.generate_report(split_data)
            # print(data)
            # result += data
        # Закрытие соединения
        sock.close()
        with open('my.json', 'w') as file:
            json.dump(report_generator.report, file, indent=3)

database = DatabaseConnection()
def new_hope(group_by_1, group_by_2, group_by_3):
    with open('my.json', 'r') as file:
        report = json.load(file)
    # Выбираем поле для первой группировки
    #group_by_1 = 'SourceIP'
    # Выбираем поле для второй группировки (если необходимо)
    #group_by_2 = 'URL'
    #group_by_3 = 'TimeInterval'
    result = {}
    for item in report:
        key_1 = item[group_by_1]
        if key_1 not in result:
            result[key_1] = {'Count': 0}
        result[key_1]['Count'] += item['Count']
        if group_by_2:
            key_2 = item[group_by_2]
            if key_2 not in result[key_1]:
                result[key_1][key_2] = {'Count': 0}
            result[key_1][key_2]['Count'] += item['Count']
            if group_by_3:
                key_3 = item[group_by_3]

                if key_3 not in result[key_1][key_2]:
                    result[key_1][key_2][key_3] = {'Count': 0}

                result[key_1][key_2][key_3]['Count'] += item['Count']
    json_data = json.dumps(result, indent = 2)
    print(json_data)
    return


@app.route('/rep', methods=['GET', 'POST'])
def dbGive(): # обращаемся к базе данных
    if request.method == 'POST':
        group_by_1 = request.form.get('group_by_1')
        group_by_2 = request.form.get('group_by_2')
        group_by_3 = request.form.get('group_by_3')
        database.receive_message()
        #json_report = json.dumps(report)
        new_hope(group_by_1, group_by_2, group_by_3)
        return "Cмотрите в терминале программы"
    return render_template('glav.html')

@app.route('/', methods=['POST'])
def receive_json():
    json_data = request.get_json()  # Получаем JSON данные из запрос
    #print(json_data)
    message = json_data['ip_address'] + " " + json_data['url'] + " " + json_data['time']
    database.send_message(message)
    return 'Received JSON data'

if __name__ == '__main__':
    app.run(port=5001, use_reloader=False)