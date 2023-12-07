#include <winsock2.h>
#include <stdio.h>
#pragma comment(lib, "ws2_32.lib")
#include <iostream>
#include <thread>
#include <fstream>
#include <string>
#include <mutex>
using namespace std;
mutex mtx1;
SOCKET Connections[100];
int Counter = 0;
struct Node { // под односвязное
    string data;
    Node* next;
};
struct HNode { // под хеш таблицу
    string key;
    string data;
    HNode* next;
};
struct HashTable { // под хеш таблицу
    HNode Table[6];
};
// под стек
void push(Node** head, string& new_data) {//добавление 
    Node* new_node = new Node();
    new_node->data = new_data;
    new_node->next = *head;
    *head = new_node;
}

string pop(Node** head) {//удаление может тоже сделать бул
    if (*head == nullptr) {
        return "Stack is empty";
    }
    Node* temp = *head;
    string popped_data = temp->data;
    *head = temp->next;
    delete temp;
    return popped_data;
}
//под очередь
void enqueue(Node** head, Node** tail, string& new_data) {//добавление
    Node* new_node = new Node();
    if (*head == nullptr) {
        *head = new_node;
        *tail = new_node;
    }
    new_node->data = new_data;
    (*tail)->next = new_node;
    new_node->next = nullptr;
    *tail = new_node;
}
string dequeue(Node** head) {//удаление
    if (*head == nullptr) {
        return "Queue is empty";
    }
    Node* temp = *head;
    string popped_data = temp->data;
    *head = temp->next;
    delete temp;
    return popped_data;
}

int trans(string& str) { // вычисление хеш функции
    int key = 0;
    for (char s : str) {
        key += static_cast<int>(s);
    }
    return key % 6;
}
void create(HNode** head, string& key, string& value) { // создание элемента списка в случае коллизии для хеш таблицы
    (*head)->key = key;
    (*head)->data = value;
    (*head)->next = nullptr;
}

string hash_add(HashTable* hTable, string key, string value) {//добавление
    int index = trans(key);
    if (hTable->Table[index].key == "") { // если ячейка не занята
        hTable->Table[index].key = key;
        hTable->Table[index].data = value;
        hTable->Table[index].next = nullptr;
        return "Element added";
    }
    else { // в случае коллизии
        HNode* new_node = new HNode();
        HNode* temp = hTable->Table[index].next;
        if (temp == nullptr) { // если это первый случай с этим адресом
            if (hTable->Table[index].key == key) { // если уже есть элемент с таким же ключом
                if (hTable->Table[index].data == value) { // если уже есть элемент с таким же ключом
                    return "There was already an element here"; // выводим ошибку
                }
                return "There is already such hash"; // выводим ошибку
            }
            hTable->Table[index].next = new_node;
            create(&new_node, key, value); // создаём элемент списка

            return "Element added";
        }
        else { // если не первый случай коллизии
            while (temp->next != nullptr) { // если следующая ячейка пуста - выходим
                if (temp->key == key) { // если ключи совпали
                    delete new_node; // освобождаем память
                    if (temp->data == value) {
                        return "There was already an element here";
                    }
                    
                    return "There is already such hash";
                }
                temp = temp->next; // переходим дальше
            }
            if (temp->key == key) { // если ключи совпали
                if (temp->key == key) { // если ключи совпали
                    delete new_node; // освобождаем память
                    if (temp->data == value) {
                        return "There was already an element here";
                    }
                    return "There is already such hash";
                }
            }
            temp->next = new_node; // обновляем указатель предыдущего
            create(&new_node, key, value); // создаём элемент списка
            return "Element added";
        }
    }
}

string hash_find(HashTable* hTable, string key) {//поиск
    int index = trans(key);  // получаем хеш функцию
    HNode* temp = hTable->Table[index].next;
    if (hTable->Table[index].key == "") { // если пустая ячейка
        return "There is no such element!";
    }
    else if (hTable->Table[index].key == key) { // если находится в хеш таблице
        return hTable->Table[index].data;
    }
    else {
        while (temp != nullptr) { // пока указатель ненулевой
            if (temp->key == key) {
                return temp->data;
            }
            temp = temp->next; // перехдим дальше
        }
        return "There is no such element!";
    }
}
// строки
bool extractStringFromFile(string& way, string& keyword, Node** head, Node** tail, string& result) {// достаём строку из файла
    string line;
    ifstream fin;
    bool found = false;//для случаев удаления, если нет такой строки в файле
    //mtx.lock();
    fin.open(way);//открываем
    if (fin.is_open()) {//если удалось открыть файл
        while (true) {
            line = "";
            getline(fin, line);//считываем из файла
            if (line.substr(0, line.find(" ")) == keyword) { // если контейнер существует

                result = line.substr(keyword.length() + 1);
                found = true;
            }
            else {

                if (line == "") {
                    break;
                }
                enqueue(&(*head), &(*tail), line); // остальные строки закидываем в очередь
            }
            if (fin.eof()) break;//если достигнут конец файла - выходим
            //cout << str << endl;//выводим в консоль
        }
    }
    if (result.empty()) {
        found = false; // если контейнер был пуст
    }
    fin.close();
    return found;
}


void extractStringFromString2(string& str, string& subs) { // разделение строки на две строки
    size_t right = str.find(" ");
    subs = str.substr(0, right); // Получаем строку до пробела
    if (right != string::npos) {
        str = str.substr(subs.size() + 1); // Получаем строку после пробела
    }
    else {
        str = "";
    }
}
// запись
string strFromHash(HashTable* hTable) { // делаем строку для записи в файл из хеш таблицы
    string result;
    for (int i = 0; i < 6; i++) {// проходимя по всем элементам
        if (!(hTable->Table[i].data).empty()) {
            result += hTable->Table[i].key;
            result += " ";
            result += hTable->Table[i].data;
            result += " ";
            if (hTable->Table[i].next != nullptr) { // если были коллизии
                HNode* temp = hTable->Table[i].next;
                while (temp != nullptr) {
                    result += temp->key;
                    result += " ";
                    result += temp->data;
                    result += " ";
                    temp = temp->next;
                }
            }
        }
    }
    cout << "strFromHash" << result << endl;
    return result.substr(0, result.size() - 1); // удаляем лишний пробел
}


void recordFile(Node** head, string& result, string& way, string& command) { // запись в файл
    ofstream fout;
    Node* temp = *head;
    fout.open(way);
    if (fout.is_open()) {
        while (temp != nullptr) {// проходимся по списку, пока не встретим указатель равный 0
            if (!((temp)->data).empty()) {
                fout << (temp)->data << endl; //вывод
                
            }
            temp = temp->next; // переход к следующему
        }
        fout << result;//запись строки с которой работали
        //mtx.unlock();
    }
    else {
        command = "Error"; // если не смогли открыть
        //mtx.unlock();
    }
}
int container(string& command, Node** headQ) { // вернём в этой же строке ответ
    
    string file, query, destin = "myhash", value, key; // на что разобъём строку
    Node* head = nullptr;
    Node* tail = nullptr;
    string filestr; // строка из файла
    string subs;
    extractStringFromString2(command, query); // разделение строки
    
    
    // Обработка аргументов командной строки
    int i = 0;
    if (query == "HSET" || query == "HGET") {
        file = "file.txt";
        extractStringFromString2(command, value);
        extractStringFromString2(command, key);
        ifstream fin;// обработка ошибки с файлом (потом раскомментировать)
        fin.open(file);//открываем
        if (!(fin.is_open())) {
            command = "We can't open this file!";
            return 1;
        }
        fin.close();
    }
    if (query == "HSET") {
        if (value.empty() || key.empty()) {
            command = "Passed wrong arguments!";;
            return 1;
        }
        HashTable hTable;
        string subkey;
        extractStringFromFile(file, destin, &head, &tail, filestr);
        int k = 0;
        while (!filestr.empty()) { // пока есть

            if (k == 0) {
                extractStringFromString2(filestr, subkey);
                k = 1;
            }
            else {
                extractStringFromString2(filestr, subs);
                hash_add(&hTable, subkey, subs);
                k = 0;
            }
        }
        command = hash_add(&hTable, value, key);
        cout << command << endl;
        filestr = strFromHash(&hTable);
        destin += " ";
        string result = destin + filestr;
        //cout << "res" << result << endl;
        recordFile(&head, result, file, command);
    }
    else if (query == "HGET") {
        if (value.empty()) {
            command = "Passed wrong arguments!";
            return 1;
        }
        if (extractStringFromFile(file, destin, &head, &tail, filestr)) {// если есть с таким именем структуры
            //mtx.unlock();
            HashTable hTable;
            string subkey;
            int k = 0;
            while (!filestr.empty()) { // пока есть
                if (k == 0) {
                    extractStringFromString2(filestr, subkey);
                    k = 1;
                }
                else {
                    extractStringFromString2(filestr, subs);
                    hash_add(&hTable, subkey, subs);
                    k = 0;
                }
            }
            command = hash_find(&hTable, value);
            cout << command << endl;
        }
        else {
            command = "We can't do it!";
            //mtx.unlock();
        }
    }
    else if (query == "QPUSH") {
        
        std::ofstream file;
        file.open("dbstat.txt", std::ios::app); // Открываем файл для дозаписи

        if (file.is_open()) {
            // Здесь можно записывать данные в файл
            file << command << std::endl;

            file.close(); // Закрываем файл
            command = "We add this elment";
        }
        else {
            std::cout << "Не удалось открыть файл для дозаписи" << std::endl;
            command = "We don't add this elment";
            
        }
        //command = "We add this elment";
    }
    else if (query == "QPOP") {
        file = "dbstat.txt";
        extractStringFromFile(file, destin, &(*headQ), &tail, filestr);
    }
    
    else { // если неправильная команда
        cout << query << endl;
        command = "Incorrect data!";
    }
}
void qpop(string& command, Node** headQ) {
    command = dequeue(&(*headQ));
    cout << "QPOP" << endl;
    cout << command << endl;
    if (command == "Queue is empty") {
        *headQ == nullptr;
    }
}

void ClientHandler(int index) {
    char msg[1024];//буфер
    string command;
    int i;
    Node* head = nullptr;
    //while (true) {
        int bytesRead = recv(Connections[index], msg, sizeof(msg), NULL);// приём сообщения от клиента
        if (bytesRead > 0) {//для правильного приёма сообщения от клиента

            msg[bytesRead] = '\0'; // добавляем нулевой символ в конец
            command = msg;
            //cout << "Command: " << command << endl;
            mtx1.lock();//закрываем мьютекс
            if (command == "QPOP") {
                container(command, &head);
                do {
                    qpop(command, &head);
                    send(Connections[index], command.c_str(), command.size(), NULL);
                } while (command != "Queue is empty");

            }
            else {
                container(command, &head);//в этой функции СУБД
                send(Connections[index], command.c_str(), command.size(), NULL);
            }
            mtx1.unlock();//открываем
        }
        //send(Connections[index], command.c_str(), command.size(), NULL); // отправляем клиенту результат
    //}
}

int main() {
    WSADATA wsaData; // создаём структуру
    SOCKET serverSocket, clientSocket;
    SOCKADDR_IN serverAddr, clientAddr;
    int clientAddrSize = sizeof(clientAddr);
    // Инициализация библиотеки Winsock
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {//загрузка необходимой версии библиотеки
        printf("Failed to initialize winsock.\n");
        return 1;
    }

    // Создание серверного сокета
    if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET) {
        printf("Failed to create socket.\n");
        WSACleanup();
        return 1;
    }

    // Настройка адреса сервера
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serverAddr.sin_port = htons(6379);

    // Привязка серверного сокета к адресу и порту
    if (bind(serverSocket, (SOCKADDR*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        printf("Failed to bind socket.\n");
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    // Перевод сокета в режим прослушивания
    if (listen(serverSocket, SOMAXCONN) == SOCKET_ERROR) {
        printf("Listen failed.\n");
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    printf("Server listening on port 6379.\n");
    SOCKET newConnection;
    for (int i = 0; i < 100; i++) {

        newConnection = accept(serverSocket, (SOCKADDR*)&clientAddr, &clientAddrSize); // принимаем соединение
        if (newConnection == INVALID_SOCKET) {// если ошибка
            printf("Accept failed.\n");
            break;
        }
        else {
            cout << "Client Connected!\n";
            Connections[0] = newConnection; // массив для хранения 
            Counter++;

            CreateThread(NULL, NULL, (LPTHREAD_START_ROUTINE)ClientHandler, (LPVOID)(0), NULL, NULL); // создание потока
        }
    }
    // закрытие серверного сокета
    closesocket(serverSocket);

    // Освобождение библиотеки Winsock
    WSACleanup();

    return 0;
}
