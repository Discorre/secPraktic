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
    char buffer[1024] = {0};

    // Получаем имя пользователя и имя хоста
    std::string username = getUsername();
    std::string hostname = getHostname();

    while (true) {

        // Отправляем приглашение с именем пользователя и хоста
        std::string prompt = username + "@" + hostname + "# ";
        sendToClient(clientSocket, prompt);

        int bytesRead = read(clientSocket, buffer, 1024);
        if (bytesRead <= 0) break;

        std::string query(buffer, bytesRead);
        query.erase(query.find_last_not_of("\r\n") + 1); // Удаление символов конца строки
        std::cout << "Received query: " << query << std::endl;

        if (query == "exit") {
            std::cout << "Client disconnected." << std::endl;
            sendToClient(clientSocket, "exit\n");
            break;
        }

        // Защищаем структуру БД с помощью мьютекса
        std::lock_guard<std::mutex> lock(dbMutex);
        parsingQuery(query, filePath, namesOfSchema, limitOfTuples, jsonStructure, clientSocket);

        std::string response = "Query processed\n";
        send(clientSocket, response.c_str(), response.size(), 0);
    }

    close(clientSocket);
}

// Ввод имени файла и директории
int InputNames(string& jsonFileName, string& filePath) {
    while (true) {
        cout << "Введите имя json файла: ";
        getline(cin, jsonFileName);
        cout << "Введите путь к файлу: ";
        getline(cin, filePath);

        try {
            if (!filesystem::exists(filePath + "\\" + jsonFileName)) {
                throw std::runtime_error("Файл JSON не найден");  // Выбрасываем ошибку, если файл не найден
            } else {
                return 0;
            }
        } catch (const exception& err) {
            cerr << "Ошибка: " << err.what() << endl;  // Выводим ошибку, если она возникла
        }
    }
}

int main() {

    int serverSocket, clientSocket;
    struct sockaddr_in serverAddress;
    struct sockaddr_in clientAddress;
    socklen_t clientAddrLen = sizeof(clientAddress);
    int opt = 1;

    // Создаем серверный сокет
    if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Устанавливаем опции сокета
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;  // Любой адрес интерфейса
    serverAddress.sin_port = htons(7432);  // Привязываем к порту 7432

    // Привязываем сокет к адресу и порту
    if (bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    // Начинаем слушать соединения
    if (listen(serverSocket, 3) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    std::cout << "Server listening on port 7432..." << std::endl;

    std::string jsonFileName = "schema.json";
    std::string filePath = ".";
    MyHashMap<std::string, MyVector<std::string>*>* jsonStructure = CreateMap<std::string, MyVector<std::string>*>(10, 50, clientSocket);

    // Чтение структуры JSON
    int limitOfTuples = 0;
    std::string namesOfSchema = readJsonFile(jsonFileName, filePath, limitOfTuples, *jsonStructure, clientSocket);

    // Цикл для принятия новых соединений
    while (true) {
        if ((clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddress, &clientAddrLen)) < 0) {
            perror("Accept failed");
            exit(EXIT_FAILURE);
        }

        std::cout << "New connection from " << inet_ntoa(clientAddress.sin_addr) << ":" << ntohs(clientAddress.sin_port) << std::endl;

        // Создаем новый поток для обработки запроса клиента
        std::thread clientThread(handleClient, clientSocket, filePath, namesOfSchema, limitOfTuples, std::ref(*jsonStructure));
        clientThread.detach();  // Отсоединяем поток, чтобы он работал независимо
    }

    DestroyMap<std::string, MyVector<std::string>*>(*jsonStructure);
    return 0;
}