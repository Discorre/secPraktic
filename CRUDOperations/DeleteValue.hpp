#ifndef DELETEVALUE_HPP
#define DELETEVALUE_HPP

#include <iostream>
#include <fstream>
#include <filesystem>
#include <string>

#include "../CustomStructures/MyVector.hpp"
#include "../CustomStructures/MyHashMap.hpp"

#include "../Other/Utilities.hpp"

#include "WhereValue.hpp"

using namespace std;

// Функция для удаления данных из таблиц
inline void removeData(MyVector<string>& namesOfTable, MyVector<string>& listOfCondition, const string& nameOfSchema, const string& path, const MyHashMap<string, MyVector<string>*>& jsonStructure, int clientSocket) {
    // Создаем дерево условий для фильтрации строк
    Node* nodeWere = getConditionTree(listOfCondition);

    // Проходим по всем таблицам, указанным в запросе
    for (int i = 0; i < namesOfTable.length; i++) {
        int fileIndex = 1;

        // Блокируем таблицу для исключения конфликтов при записи
        try {
            CheckTableLock(path + "/" + nameOfSchema + "/" + namesOfTable.data[i], namesOfTable.data[i] + "_lock.txt", 1, clientSocket);
        } catch (const std::exception& e) {
            //cerr << err.what() << endl;
            sendToClient(clientSocket, e.what());
            sendToClient(clientSocket, "\n");
            return;
        }

        // Проходим по всем файлам таблицы
        while (filesystem::exists(path + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(fileIndex) + ".csv")) {
            ifstream file(path + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(fileIndex) + ".csv");
            if (!file.is_open()) {
                //throw runtime_error("Ошибка открытия: " + (path + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(fileIndex) + ".csv"));
                sendToClient(clientSocket, "Ошибка открытия: " + (path + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(fileIndex) + ".csv") + "\n");
            }

            // Создаем временный файл для записи отфильтрованных данных
            ofstream tempFile(path + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(fileIndex) + "_temp.csv");

            string line;
            // Копируем заголовок таблицы во временный файл
            getline(file, line);
            tempFile << line;

            // Обрабатываем каждую строку таблицы
            while (getline(file, line)) {
                MyVector<string>* row = splitRow(line, ',');
                try {
                    // Проверяем, соответствует ли строка условиям
                    if (!isValidRow(nodeWere, *row, jsonStructure, namesOfTable.data[i], clientSocket)) {
                        tempFile << endl << line;
                    }
                } catch (const exception& e) {
                    //cerr << e.what() << endl;
                    sendToClient(clientSocket, e.what());
                    sendToClient(clientSocket, "\n");
                    tempFile.close();
                    file.close();
                    std::remove((path + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(fileIndex) + "_temp.csv").c_str());
                    return;
                }
            }

            // Закрываем файлы
            tempFile.close();
            file.close();
            

            // Удаляем исходный файл и переименовываем временный файл
            if (std::remove((path + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(fileIndex) + ".csv").c_str()) != 0) {
                //std::cerr << "Ошибка удаления файла" << std::endl;
                sendToClient(clientSocket, "Ошибка удаления файла\n");
                return;
            }
            if (std::rename((path + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(fileIndex) + "_temp.csv").c_str(), (path + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(fileIndex) + ".csv").c_str()) != 0) {
                //std::cerr << "Ошибка присвоения названия файлу" << std::endl;
                sendToClient(clientSocket, "Ошибка удаления файла\n");
                return;
            }

            fileIndex++;
        }

        // Разблокируем таблицу
        CheckTableLock(path + "/" + nameOfSchema + "/" + namesOfTable.data[i], namesOfTable.data[i] + "_lock.txt", 0, clientSocket);
    }
}

// Функция для парсинга DELETE запроса
void parseDelete(const MyVector<string>& words, const string& filePath, const string& nameOfSchema, const MyHashMap<string, MyVector<string>*>& jsonStructure, int clientSocket) {
    // Создаем векторы для хранения имен таблиц и условий
    MyVector<string>* namesOfTable = CreateVector<string>(5, 50);
    MyVector<string>* listOfCondition = CreateVector<string>(5, 50);
    int countTabNames = 0;
    int countWereData = 0;
    bool afterWhere = false;

    // Проходим по всем словам в запросе
    for (int i = 2; i < words.length; i++ ) {
        // Убираем запятые в конце имен таблиц
        if (words.data[i][words.data[i].size() - 1] == ',') {
            words.data[i] = getSubstring(words.data[i], 0, words.data[i].size() - 1);
        }

        // Определяем, где начинаются условия
        if (words.data[i] == "WHERE") {
            afterWhere = true;
        } else if (afterWhere) {
            // Добавляем условия в список
            AddVector<string>(*listOfCondition, words.data[i]);
            countWereData++;
        } else {
            // Добавляем имена таблиц в список
            countTabNames++;
            try {
                GetMap(jsonStructure, words.data[i], clientSocket);
            } catch (const exception& err) {
                //cerr << e.what() << ": таблица " << words.data[i] << " отсутствует." << endl;
                sendToClient(clientSocket, "Таблица " + words.data[i] + " отсутствует." + "\n");
                return;
            }
            AddVector<string>(*namesOfTable, words.data[i]);
        }
    }

    // Проверяем, что указаны и таблицы, и условия
    if (countTabNames == 0 || countWereData == 0) {
        //throw runtime_error("Отсутствует имя таблицы или данные в WHERE");
        sendToClient(clientSocket, "Отсутствует имя таблицы или данные в WHERE\n");
    }

    // Вызываем функцию для удаления данных
    try {
        removeData(*namesOfTable, *listOfCondition, nameOfSchema, filePath, jsonStructure, clientSocket);
    } catch (const exception& e) {
        //cerr << e.what()<< endl;
        sendToClient(clientSocket, e.what());
        sendToClient(clientSocket, "\n");
        return;
    }
}

#endif