#ifndef JSONPARSER_HPP
#define JSONPARSER_HPP

#include "../CustomStructures/MyVector.hpp"
#include "../CustomStructures/MyHashMap.hpp"
#include "../CustomStructures/json.hpp"
#include "Utilities.hpp"

#include <fstream>
#include "filesystem"

using json = nlohmann::json;

using namespace std;

// Создание директории
void createDirectory(const string& pathToDir) {
    filesystem::__cxx11::path path(pathToDir);
    if (!filesystem::exists(path)) {
        filesystem::create_directories(path);  // Создаем директорию, если она не существует
    }
}

// Создание файла с данными
void createFileData(const string& pathToFile, const string& fileName, const string& data, bool isDirectory, int clientSocket) {
    filesystem::path path(pathToFile);
    if (filesystem::exists(path / fileName)) {
        if (isDirectory) {
            ifstream file(path / fileName);
            string line;
            getline(file, line);
            if (line == data) {
                file.close();
                return;  // Данные уже есть в файле
            }
            file.close();
        } else {
            return;
        }
    }
    // Если данные в файле не совпадают с JSON или отсутствуют
    ofstream lockFile(path / fileName);
    if (lockFile.is_open()) {
        lockFile << data;  // Записываем данные в файл
        lockFile.close();
    } else {
        //throw runtime_error("Не удалось создать файл блокировки в директории");  // Выбрасываем ошибку, если не удалось создать файл
        sendToClient(clientSocket, "Не удалось создать файл блокировки в директории");
    }
}

// Чтение json файла и создание директорий
string readJsonFile(const string& fileName, const string& filePath, int& tuplesLimit, MyHashMap<string, MyVector<string>*>& jsonStructure, int clientSocket) {
    ifstream file(filePath + "/" + fileName);
    if (!file.is_open()) {
        //throw runtime_error("Не удалось открыть " + fileName);  // Выбрасываем ошибку, если не удалось открыть файл
        sendToClient(clientSocket, "Не удалось открыть " + fileName);
    }

    // Чтение json
    json schema;
    file >> schema;

    // Чтение имени таблицы
    string schemaName = schema["name"];
    createDirectory(schemaName);  // Создаем директорию для таблицы

    // Чтение максимального количества ключей
    tuplesLimit = schema["tuples_limit"];

    // Чтение структуры таблицы
    json tableStructure = schema["structure"];
    for (auto& [key, value] : tableStructure.items()) {
        // Создание директорий
        createDirectory(schemaName + "/" + key);
        MyVector<string>* tempValue = CreateVector<string>(10, 50);  // Создаем вектор для имен столбцов
        string colNames = key + "_pk";
        for (auto columns : value) {
            colNames += ",";
            string temp = columns;
            colNames += temp;
            AddVector(*tempValue, temp);  // Добавляем имя столбца в вектор
        }
        createFileData(schemaName + "/" + key, "1.csv", colNames, true, clientSocket);  // Создаем файл с именами столбцов
        createFileData(schemaName + "/" + key, key + "_lock.txt", "0", false, clientSocket);  // Создаем файл блокировки
        createFileData(schemaName + "/" + key, key + "_pk_sequence.txt", "0", false, clientSocket);  // Создаем файл последовательности первичных ключей
        AddMap<string, MyVector<string>*>(jsonStructure, key, tempValue);  // Добавляем структуру таблицы в хэш-таблицу
    }

    file.close();
    return schemaName;
}

#endif //READJSON_H