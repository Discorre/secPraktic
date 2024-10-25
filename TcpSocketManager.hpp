#ifndef TCP_SOCKET_MANAGER_HPP
#define TCP_SOCKET_MANAGER_HPP

#include <iostream>
#include <arpa/inet.h>
#include <unistd.h>

class TcpSocketManager {
public:
    static int serverSocket;
    static int clientSocket;

    static bool initialize(int port);
    static void closeSockets();
};

// Инициализация статических переменных
int TcpSocketManager::serverSocket = -1;
int TcpSocketManager::clientSocket = -1;

bool TcpSocketManager::initialize(int port) {
    // Создание сокета
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket < 0) {
        std::cerr << "Ошибка при создании сокета" << std::endl;
        return false;
    }

    // Настройка адреса
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY; // Использовать любой доступный интерфейс
    serverAddr.sin_port = htons(port); // Преобразование порта в сетевой порядок байтов

    // Привязка сокета к адресу
    if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        std::cerr << "Ошибка при привязке сокета" << std::endl;
        close(serverSocket);
        return false;
    }

    // Прослушивание входящих соединений
    if (listen(serverSocket, 5) < 0) {
        std::cerr << "Ошибка при прослушивании" << std::endl;
        close(serverSocket);
        return false;
    }

    std::cout << "Сервер запущен на порту " << port << "..." << std::endl;
    return true;
}

void TcpSocketManager::closeSockets() {
    if (clientSocket >= 0) {
        close(clientSocket);
    }
    if (serverSocket >= 0) {
        close(serverSocket);
    }
}

#endif // TCP_SOCKET_MANAGER_HPP
