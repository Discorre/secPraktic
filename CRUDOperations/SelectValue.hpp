#ifndef SELECTVALUE_HPP
#define SELECTVALUE_HPP

#include <iostream>
#include <fstream>
#include <filesystem>

#include "../CustomStructures/MyHashMap.hpp"
#include "../CustomStructures/MyVector.hpp"

#include "../Other/Utilities.hpp"

#include "WhereValue.hpp"

using namespace std;

// Функция для чтения таблицы из файла
MyVector<MyVector<string>*>* ReadTable(const string& namesOfTable, const string& namesOfSchema, const string& pathFile, const MyVector<string>& namesOfColumns, const MyVector<string>& listOfCondition, const MyHashMap<string, MyVector<string>*>& jsonStructure, bool whereValue, int clientSocket) {
    // Создание вектора для хранения данных таблицы
    MyVector<MyVector<string>*>* dataOfTable = CreateVector<MyVector<string>*>(5, 50);
    int indexFile = 1;
    try {
        // Блокировка таблицы для чтения
        CheckTableLock(pathFile + "/" + namesOfSchema + "/" + namesOfTable, namesOfTable + "_lock.txt", 1, clientSocket);
    } catch (const std::exception& e) {
        // Вывод ошибки, если не удалось заблокировать таблицу
        //cerr << e.what() << endl;
        sendToClient(clientSocket, e.what());
        sendToClient(clientSocket, "\n");
        return dataOfTable;
    }
    // Получение дерева условий для whereValue
    Node* nodeWere = getConditionTree(listOfCondition);
    // Цикл по всем файлам таблицы
    while (filesystem::exists(pathFile + "/" + namesOfSchema + "/" + namesOfTable + "/" + to_string(indexFile) + ".csv")) {
        // Открытие файла для чтения
        ifstream file(pathFile + "/" + namesOfSchema + "/" + namesOfTable + "/" + to_string(indexFile) + ".csv");
        if (!file.is_open()) {
            // Выброс исключения, если файл не открылся
            //throw runtime_error("Ошибка открытия файла" + (pathFile + "/" + namesOfSchema + "/" + namesOfTable + "/" + to_string(indexFile) + ".csv"));
            sendToClient(clientSocket, "Ошибка открытия файла" + (pathFile + "/" + namesOfSchema + "/" + namesOfTable + "/" + to_string(indexFile) + ".csv" + "\n"));
        }
        string firstLine;
        // Чтение первой строки файла (заголовка)
        getline(file, firstLine);
        // Если выбраны все столбцы
        if (namesOfColumns.data[0] == "*") {
            string line;
            // Чтение всех строк файла
            while (getline(file, line)) {
                // Разделение строки на столбцы
                MyVector<string>* row = splitRow(line, ',');
                if (whereValue) {
                    try {
                        // Проверка строки на соответствие условиям whereValue
                        if (isValidRow(nodeWere, *row, jsonStructure, namesOfTable, clientSocket)) {
                            // Удаление первого столбца (обычно это ID)
                            DeleteVector<string>(*row, 0);
                            // Добавление строки в результат
                            AddVector(*dataOfTable, row);
                        }
                    } catch (const exception& e) {
                        // Вывод ошибки и закрытие файла
                        //cerr << e.what() << endl;
                        sendToClient(clientSocket, e.what());
                        sendToClient(clientSocket, "\n");
                        file.close();
                        return dataOfTable;
                    }
                } else {
                    // Удаление первого столбца (обычно это ID)
                    DeleteVector<string>(*row, 0);
                    // Добавление строки в результат
                    AddVector(*dataOfTable, row);
                }
            }
        } else {
            // Получение имен столбцов из JSON структуры
            MyVector<string>* filenamesOfColumns = GetMap<string, MyVector<string>*>(jsonStructure, namesOfTable, clientSocket);
            // Создание вектора для хранения индексов выбранных столбцов
            MyVector<int>* colIndex = CreateVector<int>(10, 50);
            // Заполнение вектора индексов выбранных столбцов
            for (int i = 0; i < filenamesOfColumns->length; i++) {
                for (int j = 1; j < namesOfColumns.length; j++) {
                    if (filenamesOfColumns->data[i] == namesOfColumns.data[j]) {
                        AddVector(*colIndex, i + 1);
                    }
                }
            }
            string line;
            // Чтение всех строк файла
            while (getline(file, line)) {
                // Разделение строки на столбцы
                MyVector<string>* row = splitRow(line, ',');
                if (whereValue) {
                    try {
                        // Проверка строки на соответствие условиям whereValue
                        if (isValidRow(nodeWere, *row, jsonStructure, namesOfTable, clientSocket)) {
                            // Создание новой строки с выбранными столбцами
                            MyVector<string>* newRow = CreateVector<string>(colIndex->length, 50);
                            for (int i = 0; i < colIndex->length; i++) {
                                AddVector(*newRow, row->data[colIndex->data[i]]);
                            }
                            // Добавление новой строки в результат
                            AddVector(*dataOfTable, newRow);
                        }
                    } catch (const exception& e) {
                        // Вывод ошибки и закрытие файла
                        //cerr << e.what() << endl;
                        sendToClient(clientSocket, e.what());
                        sendToClient(clientSocket, "\n");
                        file.close();
                        return dataOfTable;
                    }
                } else {
                    // Создание новой строки с выбранными столбцами
                    MyVector<string>* newRow = CreateVector<string>(colIndex->length, 50);
                    for (int i = 0; i < colIndex->length; i++) {
                        AddVector(*newRow, row->data[colIndex->data[i]]);
                    }
                    // Добавление новой строки в результат
                    AddVector(*dataOfTable, newRow);
                }
            }
        }
        // Закрытие файла
        file.close();
        // Переход к следующему файлу
        indexFile += 1;
    }
    // Разблокировка таблицы
    CheckTableLock(pathFile + "/" + namesOfSchema + "/" + namesOfTable, namesOfTable + "_lock.txt", 1, clientSocket);
    // Возврат данных таблицы
    return dataOfTable;
}

// Вывод содержимого таблиц в виде декартового произведения
void CartesianProduct(const MyVector<MyVector<MyVector<string>*>*>& tablesData, MyVector<MyVector<string>*>& temp, int counterTab, int tab, int clientSocket) {
    // Цикл по всем строкам текущей таблицы
    for (int i = 0; i < tablesData.data[counterTab]->length; i++) {
        // Добавление текущей строки в временный вектор
        temp.data[counterTab] = tablesData.data[counterTab]->data[i];
        // Рекурсивный вызов для следующей таблицы
        if (counterTab < tab - 1) {
            CartesianProduct(tablesData, temp, counterTab + 1, tab, clientSocket);
        } else {
            // Вывод декартового произведения
            for (int j = 0; j < tab; j++) {
                //cout << *temp.data[j] << std::setw(25);
                formatAndSendData(clientSocket, *temp.data[j]);
            }
            cout << endl;
        }
    }
    return;
}

// Подготовка к чтению и выводу данных
void selectDataPreparation(const MyVector<string>& namesOfColumns, const MyVector<string>& namesOfTables, const MyVector<string>& listOfCondition, const string& namesOfSchema, const string& pathFile, const MyHashMap<string, MyVector<string>*>& jsonStructure, bool whereValue, int clientSocket) {
    // Создание вектора для хранения данных таблиц
    MyVector<MyVector<MyVector<string>*>*>* tablesData = CreateVector<MyVector<MyVector<string>*>*>(10, 50);
    // Если выбраны все столбцы
    if (namesOfColumns.data[0] == "*") {
        // Чтение всех данных из таблиц
        for (int j = 0; j < namesOfTables.length; j++) {
            MyVector<MyVector<string>*>* tableData = ReadTable(namesOfTables.data[j], namesOfSchema, pathFile, namesOfColumns, listOfCondition, jsonStructure, whereValue, clientSocket);
            AddVector(*tablesData, tableData);
        }
    } else {
        // Чтение данных из выбранных столбцов
        for (int i = 0; i < namesOfTables.length; i++) {
            MyVector<string>* tabColPair = CreateVector<string>(5, 50);
            AddVector(*tabColPair, namesOfTables.data[i]);
            for (int j = 0; j < namesOfColumns.length; j++) {
                MyVector<string>* splitnamesOfColumns = splitRow(namesOfColumns.data[j], '.');
                try {
                    GetMap(jsonStructure, splitnamesOfColumns->data[0], clientSocket);
                } catch (const exception& e) {
                    //cerr << e.what() << ": Tаблица " << splitnamesOfColumns->data[0] << " отсутствует" << endl;
                    sendToClient(clientSocket, "Tаблица " + splitnamesOfColumns->data[0] + " отсутствует" + "\n");
                    return;
                }
                if (splitnamesOfColumns->data[0] == namesOfTables.data[i]) {
                    AddVector(*tabColPair, splitnamesOfColumns->data[1]);
                }
            }
            MyVector<MyVector<string>*>* tableData = ReadTable(tabColPair->data[0], namesOfSchema, pathFile, *tabColPair, listOfCondition, jsonStructure, whereValue, clientSocket);
            AddVector(*tablesData, tableData);
        }
    }
    // Создание временного вектора для хранения строк
    MyVector<MyVector<string>*>* temp = CreateVector<MyVector<string>*>(tablesData->length * 2, 50);
    // Вывод декартового произведения
    CartesianProduct(*tablesData, *temp, 0, tablesData->length, clientSocket);
}

// Парсинг SELECT запроса
void parseSelect(const MyVector<string>& words, const string& pathFile, const string& namesOfSchema, const MyHashMap<string, MyVector<string>*>& jsonStructure, int clientSocket) {
    // Создание векторов для хранения имен столбцов, таблиц и условий whereValue
    MyVector<string>* namesOfColumns = CreateVector<string>(10, 50);
    MyVector<string>* namesOfTables = CreateVector<string>(10, 50);
    MyVector<string>* listOfCondition = CreateVector<string>(10, 50);
    bool afterFrom = false;
    bool afterwhereValue = false;
    int countTabNames = 0;
    int countData = 0;
    int countWhereValueData = 0;
    // Цикл по всем словам запроса
    for (int i = 1; i < words.length; i++) {
        // Удаление запятой в конце слова, если она есть
        if (words.data[i][words.data[i].size() - 1] == ',') {
            words.data[i] = getSubstring(words.data[i], 0, words.data[i].size() - 1);
        }
        // Определение разделителей в запросе
        if (words.data[i] == "FROM") {
            afterFrom = true;
        } else if (words.data[i] == "WHERE") {
            afterwhereValue = true;
        } else if (afterwhereValue) {
            // Добавление условий whereValue в список
            countWhereValueData++;
            AddVector<string>(*listOfCondition, words.data[i]);
        } else if (afterFrom) {
            // Проверка наличия таблицы в JSON структуре
            try {
                GetMap(jsonStructure, words.data[i], clientSocket);
            } catch (const exception& e) {
                //cerr << e.what() << ": Tаблица " << words.data[i] << " отсутствует" << endl;
                sendToClient(clientSocket, "Tаблица " + words.data[i] + " отсутствует" + '\n');
                return;
            }
            // Добавление имени таблицы в список
            countTabNames++;
            AddVector(*namesOfTables, words.data[i]);
        } else {
            // Добавление имен столбцов в список
            countData++;
            AddVector(*namesOfColumns, words.data[i]);
        }
    }
    // Проверка наличия имен таблиц и столбцов
    if (countTabNames == 0 || countData == 0) {
        throw runtime_error("Отсутствует имя таблицы или данные в FROM");
    }
    // Вызов функции подготовки к чтению данных
    if (countWhereValueData == 0) {
        selectDataPreparation(*namesOfColumns, *namesOfTables, *listOfCondition, namesOfSchema, pathFile, jsonStructure, false, clientSocket);
    } else {
        selectDataPreparation(*namesOfColumns, *namesOfTables, *listOfCondition, namesOfSchema, pathFile, jsonStructure, true, clientSocket);
    }
}

#endif