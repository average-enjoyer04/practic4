from flask import Flask
from flask import request, redirect, render_template
from datetime import datetime, timedelta
import json
import socket
import hashlib
import requests
import validators
app = Flask(__name__)

def get_hours_minutes():
    now = datetime.now()
    next_minute = now + timedelta(minutes=1)

    time_str = now.strftime("%H:%M") + "-" + next_minute.strftime("%H:%M")
    return time_str
def database(message): # обращаемся к базе данных
    host = '127.0.0.1'
    port = 6379
    # Создание TCP сокета и установка соединения
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(message.encode()) # отправка запроса
    data = sock.recv(1024) # приём ответа
    sock.close() # закрытие сокета
    #print(data)
    return data.decode('utf-8')


@app.route('/<name>') # перенаправляем на страницу (или выводим ошибку)
def redirShort(name):
    if name == 'favicon.ico':
        return ''
    data1 = database("HGET " + name) # передаём запрос базе данных
    print(data1)
    if validators.url(data1): # если нам вернули ссылку
        ip_address = request.remote_addr
        str_url = data1 + " " + name
        time = get_hours_minutes()
        dataStat = {
            'ip_address': ip_address,
            'url': str_url,
            'time': time
        }
        "json_data = json.dumps(dataStat)"
        "print(json_data)"
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.post('http://127.0.0.1:5001', json=dataStat)  #, headers=headers)
        return redirect(data1, code=302)
    else: # если возникли проблемы
        return render_template('error.html', message=data1) #

def createUrl(link):
    shlink = hashlib.sha1(link.encode()).hexdigest()  # Вызываем hexdigest() для получения хэш-строки
    while len(shlink) >= 6:
        req = "HSET " + shlink[:6] + " " + link
        ans = database(req)
        if ans == "Element added" or ans == "There was already an element here": # если элемент уже был или успешно добавился
            return shlink[:6]
        elif ans == "There is already such hash": # если эта последовательность уже занята
            shlink = shlink[6:]
        else: # если возникли какие-то проблемы
            return ans

    return "We couldn't do it"


@app.route('/', methods=['GET', 'POST'])
def auth():
    if request.method == 'POST':
        link = request.form.get('link')
        if validators.url(link): # вывод сокращённой ссылки
            ans = "http://127.0.0.1:5000/" + createUrl(link)
            return "<h2>Here is a shortened link we have made for you: {0}</h2>".format(ans)
        else: # если ссылка неправильная
            return render_template('error.html', message="You gave us a bad link")

    return render_template('glav.html') # главное меню
if __name__ == '__main__':
    app.run(use_reloader=False)