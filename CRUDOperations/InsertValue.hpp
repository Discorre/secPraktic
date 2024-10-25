#ifndef INSERTVALUE_HPP
#define INSERTVALUE_HPP

#include <iostream>
#include <fstream>
#include <stdexcept>
#include <string>

#include "../Other/Utilities.hpp"
#include "../CustomStructures/MyHashMap.hpp"
#include "../CustomStructures/MyVector.hpp"
#include "SelectValue.hpp"

using namespace std;

// Функция для удаления апострофов из строки
string CleanText(string& str, int clientSocket) {
    // Удаление запятой и скобки в конце строки
    if (str[str.size() - 1] == ',' && str[str.size() - 2] == ')') {
        str = getSubstring(str, 0, str.size() - 2);
    } else if (str[str.size() - 1] == ',' || str[str.size() - 1] == ')') {
        str = getSubstring(str, 0, str.size() - 1);
    }

    // Удаление апострофов в начале и конце строки
    if (str[0] == '\'' && str[str.size() - 1] == '\'') {
        str = getSubstring(str, 1, str.size() - 1);
        return str;
    } else {
        //throw runtime_error("Неверный синтаксис в VALUES " + str);
        sendToClient(clientSocket, "Неверный синтаксис в VALUES " + str + "\n");
    }
}

// Функция для проверки количества аргументов относительно столбцов таблиц
void Validate(int colLen, const MyVector<string>& namesOfTable, const MyHashMap<string, MyVector<string>*>& jsonStructure, int clientSocket) {
    for (int i = 0; i < namesOfTable.length; i++) {
        MyVector<string>* temp = GetMap<string, MyVector<string>*>(jsonStructure, namesOfTable.data[i], clientSocket);
        if (temp->length != colLen) {
            //throw runtime_error("Количество аргументов не равно столбцам в " + namesOfTable.data[i]);
            sendToClient(clientSocket, "Количество аргументов не равно столбцам в " + namesOfTable.data[i] + "\n");
        }
    }
}

// Функция для чтения или записи первичного ключа
int readPrKey(const string& path, const bool rec, const int newID, int clientSocket) {
    fstream pkFile(path);
    if (!pkFile.is_open()) {
        //throw runtime_error("Не удалось открыть " + path);
        sendToClient(clientSocket, "Не удалось открыть " + path + "\n");
    }
    int lastID = 0;
    if (rec) {
        pkFile << newID;
    } else {
        pkFile >> lastID;
    }
    pkFile.close();
    return lastID;
}

// Функция для добавления строк в файл
void insertRows(MyVector<MyVector<string>*>& addNewData, MyVector<string>& namesOfTable, const string& nameOfSchema, const int limitOfTuples, const string& filePath, int clientSocket) {
    for (int i = 0; i < namesOfTable.length; i++) {
        int lastID = 0;
        try {
            CheckTableLock(filePath + "/" + nameOfSchema + "/" + namesOfTable.data[i], namesOfTable.data[i] + "_lock.txt", 1, clientSocket);
            lastID = readPrKey(filePath + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + namesOfTable.data[i] + "_pk_sequence.txt", false, 0, clientSocket);
        } catch (const std::exception& e) {
            //cerr << e.what() << endl;
            sendToClient(clientSocket, e.what());
            sendToClient(clientSocket, "\n");
            //return;
        }

        int newID = lastID;
        for (int j = 0; j < addNewData.length; j++) {
            newID++;
            string tempPath;
            if (lastID / limitOfTuples < newID / limitOfTuples) {
                tempPath = filePath + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(newID / limitOfTuples + 1) + ".csv";
            } else {
                tempPath = filePath + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + to_string(lastID / limitOfTuples + 1) + ".csv";
            }
            fstream csvFile(tempPath, ios::app);
            if (!csvFile.is_open()) {
                //throw runtime_error("Не удалось открыть " + tempPath);
                sendToClient(clientSocket, "Не удалось открыть " + tempPath + "\n");
            }
            csvFile << endl << newID;
            for (int k = 0; k < addNewData.data[j]->length; k++) {
                csvFile << "," << addNewData.data[j]->data[k];
            }
            csvFile.close();
        }
        readPrKey(filePath + "/" + nameOfSchema + "/" + namesOfTable.data[i] + "/" + namesOfTable.data[i] + "_pk_sequence.txt", true, newID, clientSocket);
        CheckTableLock(filePath + "/" + nameOfSchema + "/" + namesOfTable.data[i], namesOfTable.data[i] + "_lock.txt", 0, clientSocket);
    }
}

// Функция для парсинга команды INSERT
void parseInsert(const MyVector<string>& slovs, const string& filePath, const string& nameOfSchema, const int limitOfTuples, const MyHashMap<string, MyVector<string>*>& jsonStructure, int clientSocket) {
    MyVector<string>* targetTables = CreateVector<string>(5, 50);
    MyVector<MyVector<string>*>* dataToInsert = CreateVector<MyVector<string>*>(10, 50);
    bool afterValues = false;
    int countOfTable = 0;
    int dataCount = 0;
    for (int i = 2; i < slovs.length; i++) {
        if (slovs.data[i][slovs.data[i].size() - 1] == ',') {
            slovs.data[i] = getSubstring(slovs.data[i], 0, slovs.data[i].size() - 1);
        }
        if (slovs.data[i] == "VALUES") {
            afterValues = true;
        } else if (afterValues) {
            dataCount++;
            if (slovs.data[i][0] == '(') {
                MyVector<string>* tempData = CreateVector<string>(5, 50);
                slovs.data[i] = getSubstring(slovs.data[i], 1, slovs.data[i].size());

                while (slovs.data[i][slovs.data[i].size() - 1] != ')' && slovs.data[i][slovs.data[i].size() - 2] != ')') {
                    try {
                        CleanText(slovs.data[i], clientSocket);
                    } catch (const exception& e) {
                        //cerr << e.what() << " " << slovs.data[i] << endl;
                        sendToClient(clientSocket, e.what());
                        sendToClient(clientSocket, "\n");
                        //return;
                    }

                    AddVector<string>(*tempData, slovs.data[i]);
                    i++;
                }
                try {
                    CleanText(slovs.data[i], clientSocket);
                    AddVector<string>(*tempData, slovs.data[i]);
                    Validate(tempData->length, *targetTables, jsonStructure, clientSocket);
                } catch (const exception& e) {
                    //cerr << e.what() << endl;
                    sendToClient(clientSocket, e.what());
                    sendToClient(clientSocket, "\n");
                    //return;
                }
                AddVector<MyVector<string>*>(*dataToInsert, tempData);
            }

        } else {
            countOfTable++;
            try {
                GetMap(jsonStructure, slovs.data[i], clientSocket);
            } catch (const exception& err) {
                //cerr << e.what() << ": Таблица " << slovs.data[i] << " отсутствует" << endl;
                sendToClient(clientSocket, "Таблица " + slovs.data[i] + " отсутствует" + "\n");
                //return;
            }
            AddVector<string>(*targetTables, slovs.data[i]);
        }
    }
    if (countOfTable == 0 || dataCount == 0) {
        //throw runtime_error("Отсутствует имя таблицы или данные в VALUES");
        sendToClient(clientSocket, "Отсутствует имя таблицы или данные в VALUES\n");
    }

    try {
        insertRows(*dataToInsert, *targetTables, nameOfSchema, limitOfTuples, filePath, clientSocket);
    } catch (const exception& e) {
        //cerr << e.what() << endl;
        sendToClient(clientSocket, e.what());
        sendToClient(clientSocket, "\n");
        //return;
    }
}

#endif // INSERT_HPP