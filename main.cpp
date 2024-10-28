#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
#include <pwd.h>

#include "CustomStructures/MyVector.hpp"
#include "CustomStructures/MyHashMap.hpp"

#include "Other/JsonParser.hpp"
#include "Other/Utilities.hpp"

#include "CRUDOperations/SelectValue.hpp"
#include "CRUDOperations/InsertValue.hpp"
#include "CRUDOperations/DeleteValue.hpp"

using namespace std;

mutex dbMutex;

// Парсит и выполняет SQL-запросы
void parsingQuery(const string& query, const string& filePath, const string& namesOfSchema, const int limitOfTuples, const MyHashMap<string, MyVector<string>*>& jsonStructure, int clientSocket) {
    MyVector<string>* words = splitRow(query, ' ');  // Разбиваем запрос на слова
    if (words->data[0] == "SELECT") {
        try {
            parseSelect(*words, filePath, namesOfSchema, jsonStructure, clientSocket);  // Выполняем SELECT запрос
        } catch (const exception& e) {
            //cerr << e.what() << endl;  // Выводим ошибку, если она возникла
            sendToClient(clientSocket, e.what());
            sendToClient(clientSocket, "\n");
        }
    
    } else if (words->data[0] == "INSERT" && words->data[1] == "INTO") {
        try {
            parseInsert(*words, filePath, namesOfSchema, limitOfTuples, jsonStructure, clientSocket);  // Выполняем INSERT запрос
        } catch (const exception& e) {
            //cerr << e.what() << endl;  // Выводим ошибку, если она возникла
            sendToClient(clientSocket, e.what());
            sendToClient(clientSocket, "\n");

        }
    
    } else if (words->data[0] == "DELETE" && words->data[1] == "FROM") {
        try {
            parseDelete(*words, filePath, namesOfSchema, jsonStructure, clientSocket);  // Выполняем DELETE запрос
        } catch (const exception& e) {
            //cerr << e.what() << endl;  // Выводим ошибку, если она возникла
            sendToClient(clientSocket, e.what());
            sendToClient(clientSocket, "\n");
        }
        
    } else { 
        //cout << "Неизвестная команда" << endl;  // Выводим сообщение, если команда не распознана
        sendToClient(clientSocket, "Неизвестная команда\n");
    }
}

// Функция для получения имени пользователя
std::string getUsername() {
    struct passwd *pw = getpwuid(getuid());
    return pw ? std::string(pw->pw_name) : "Unknown";
}

// Функция для получения имени хоста
std::string getHostname() {
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) == 0) {
        return std::string(hostname);
    }
    return "Unknown";
}

void handleClient(int clientSocket, std::string filePath, std::string namesOfSchema, int limitOfTuples, MyHashMap<std::string, MyVector<std::string>*> &jsonStructure) {
    // Буфер для хранения данных, полученных от клиента
    char buffer[1024] = {0};

    // Получаем имя пользователя и хоста для отображения в приглашении
    std::string username = getUsername();
    std::string hostname = getHostname();

    // Главный цикл для обработки клиентских запросов
    while (true) {
        // Формируем приглашение с именем пользователя и хоста, например "user@host# "
        std::string prompt = username + "@" + hostname + "# ";
        sendToClient(clientSocket, prompt);  // Отправляем приглашение клиенту

        // Читаем данные, отправленные клиентом, в буфер
        int bytesRead = read(clientSocket, buffer, 1024);
        if (bytesRead <= 0) break;  // Прерываем цикл, если клиент отключился или ошибка

        // Преобразуем данные из буфера в строку
        std::string query(buffer, bytesRead);
        query.erase(query.find_last_not_of("\r\n") + 1);  // Удаляем символы конца строки
        std::cout << "Received query: " << query << std::endl;  // Выводим полученный запрос для отладки

        // Проверяем, если клиент отправил команду выхода
        if (query == "exit") {
            std::cout << "Client disconnected." << std::endl;  // Сообщение об отключении клиента
            sendToClient(clientSocket, "exit\n");  // Отправляем клиенту подтверждение завершения
            break;  // Прерываем цикл и завершаем работу с клиентом
        }

        // Защищаем общую структуру jsonStructure с помощью мьютекса для безопасного доступа
        std::lock_guard<std::mutex> lock(dbMutex);
        
        // Обработка запроса клиента: парсим и выполняем его с учетом параметров схемы, лимита записей и структуры данных
        parsingQuery(query, filePath, namesOfSchema, limitOfTuples, jsonStructure, clientSocket);

        // Отправляем клиенту подтверждение, что запрос обработан
        std::string response = "Query processed\n";
        send(clientSocket, response.c_str(), response.size(), 0);
    }

    // Закрываем сокет клиента после завершения работы
    close(clientSocket);
}


int main() {

    int serverSocket, clientSocket;  // Переменные для серверного и клиентского сокетов
    struct sockaddr_in serverAddress;  // Структура для хранения адреса сервера
    struct sockaddr_in clientAddress;  // Структура для хранения адреса клиента
    socklen_t clientAddrLen = sizeof(clientAddress);  // Размер структуры адреса клиента
    int opt = 1;  // Опция для сокета (возможность переиспользовать адрес)

    // Создаем серверный сокет
    if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket creation failed");  // Ошибка создания сокета
        exit(EXIT_FAILURE);
    }

    // Устанавливаем опции сокета, чтобы можно было переиспользовать адрес и порт
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt failed");  // Ошибка установки опций
        exit(EXIT_FAILURE);
    }

    // Заполняем структуру serverAddress, чтобы сервер принимал запросы на любом адресе (INADDR_ANY) и порте 7432
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;  // Любой доступный адрес
    serverAddress.sin_port = htons(7432);  // Порт сервера 7432

    // Привязываем сокет к адресу и порту
    if (bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        perror("Bind failed");  // Ошибка привязки
        exit(EXIT_FAILURE);
    }

    // Начинаем слушать входящие соединения с очередью до 3 подключений
    if (listen(serverSocket, 3) < 0) {
        perror("Listen failed");  // Ошибка прослушивания
        exit(EXIT_FAILURE);
    }

    std::cout << "Server listening on port 7432..." << std::endl;  // Сообщение о том, что сервер готов принимать подключения

    std::string jsonFileName = "schema.json";  // Имя JSON-файла для загрузки данных
    std::string filePath = ".";  // Путь к файлу JSON
    MyHashMap<std::string, MyVector<std::string>*>* jsonStructure = CreateMap<std::string, MyVector<std::string>*>(10, 50, clientSocket);

    // Чтение структуры JSON из файла
    int limitOfTuples = 0;  // Переменная для хранения лимита записей
    std::string namesOfSchema = readJsonFile(jsonFileName, filePath, limitOfTuples, *jsonStructure, clientSocket);

    // Цикл для принятия и обработки новых соединений
    while (true) {
        // Принимаем новое подключение от клиента
        if ((clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddress, &clientAddrLen)) < 0) {
            perror("Accept failed");  // Ошибка принятия соединения
            exit(EXIT_FAILURE);
        }

        // Сообщение о новом подключении с выводом IP-адреса и порта клиента
        std::cout << "New connection from " << inet_ntoa(clientAddress.sin_addr) << ":" << ntohs(clientAddress.sin_port) << std::endl;

        // Создаем новый поток для обработки запроса от клиента
        std::thread clientThread(handleClient, clientSocket, filePath, namesOfSchema, limitOfTuples, std::ref(*jsonStructure));
        clientThread.detach();  // Отсоединяем поток, чтобы он работал независимо и не блокировал главный поток
    }

    // Уничтожаем созданную структуру данных после завершения работы сервера
    DestroyMap<std::string, MyVector<std::string>*>(*jsonStructure);
    return 0;
}
