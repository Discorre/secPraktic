#ifndef WHEREVALUE_HPP
#define WHEREVALUE_HPP

#include <iostream>
#include <string>

#include "../CustomStructures/MyHashMap.hpp"
#include "../CustomStructures/MyVector.hpp"

#include "../Other/Utilities.hpp"
#include "SelectValue.hpp"

using namespace std;

// Определение типа узла
enum class NodeType {
    ConditionNode,  // Узел условия
    OrNode,         // Узел логического ИЛИ
    AndNode         // Узел логического И
};

// Структура узла
struct Node {
    NodeType nodeType;  // Тип узла
    MyVector<std::string> value;  // Значение узла
    Node* left;  // Левый потомок
    Node* right;  // Правый потомок

    // Конструктор узла
    Node(NodeType type, const MyVector<std::string> val = {}, Node* l = nullptr, Node* r = nullptr)
        : nodeType(type), value(val), left(l), right(r) {}
};

// Функция для удаления апострофов в начале и конце строки
std::string SanitizeText(std::string str, int clientSocket) {
    if (str[0] == '\'' && str[str.size() - 1] == '\'') {
        str = getSubstring(str, 1, str.size() - 1);  // Удаляем апострофы
        return str;
    } else {
        //throw std::runtime_error("Неверный синтаксис в WHERE " + str);  // Выбрасываем ошибку, если синтаксис неверный
        sendToClient(clientSocket, "Неверный синтаксис в WHERE " + str);
    }
}

// Вспомогательная функция для разделения строки по оператору
MyVector<MyVector<std::string>*>* splitByOperator(const MyVector<std::string>& query, const std::string& op) {
    MyVector<std::string>* left = CreateVector<std::string>(6, 50);  // Создаем вектор для левой части
    MyVector<std::string>* right = CreateVector<std::string>(6, 50);  // Создаем вектор для правой части
    bool afterOp = false;  // Флаг, указывающий на то, что оператор уже был встречен
    for (int i = 0; i < query.length; i++) {
        if (query.data[i] == op) {
            afterOp = true;  // Устанавливаем флаг, если встретили оператор
        } else if (afterOp) {
            AddVector(*right, query.data[i]);  // Добавляем элемент в правую часть
        } else {
            AddVector(*left, query.data[i]);  // Добавляем элемент в левую часть
        }
    }
    MyVector<MyVector<std::string>*>* parseVector = CreateVector<MyVector<std::string>*>(5, 50);  // Создаем вектор для результата
    if (afterOp) {
        AddVector(*parseVector, left);  // Добавляем левую часть в результат
        AddVector(*parseVector, right);  // Добавляем правую часть в результат
    } else {
        AddVector(*parseVector, left);  // Добавляем только левую часть, если оператор не был найден
    }
    return parseVector;
}

// Функция для построения дерева условий
Node* getConditionTree(const MyVector<std::string>& query) {
    MyVector<MyVector<std::string>*>* orParts = splitByOperator(query, "OR");  // Разделяем строку по оператору OR

    // Если найден оператор OR
    if (orParts->length > 1) {
        Node* root = new Node(NodeType::OrNode);  // Создаем узел OR
        root->left = getConditionTree(*orParts->data[0]);  // Рекурсивно строим левое поддерево
        root->right = getConditionTree(*orParts->data[1]);  // Рекурсивно строим правое поддерево
        return root;
    }
    // Если найден оператор AND
    MyVector<MyVector<std::string>*>* andParts = splitByOperator(query, "AND");  // Разделяем строку по оператору AND
    if (andParts->length > 1) {
        Node* root = new Node(NodeType::AndNode);  // Создаем узел AND
        root->left = getConditionTree(*andParts->data[0]);  // Рекурсивно строим левое поддерево
        root->right = getConditionTree(*andParts->data[1]);  // Рекурсивно строим правое поддерево
        return root;
    }

    // Если это простое условие
    return new Node(NodeType::ConditionNode, query);  // Создаем узел условия
}

// Функция для проверки, удовлетворяет ли строка условию
bool isValidRow(Node* node, const MyVector<std::string>& row, const MyHashMap<std::string, MyVector<std::string>*>& jsonStructure, const std::string& namesOfTable, int clientSocket) {
    if (!node) {
        return false;  // Если узел пустой, возвращаем false
    }

    switch (node->nodeType) {
    case NodeType::ConditionNode: {
        if (node->value.length != 3) {
            return false;  // Если условие не состоит из трех частей, возвращаем false
        }

        MyVector<std::string> *part1Splitted = splitRow(node->value.data[0], '.');  // Разделяем первую часть условия по точке
        if (part1Splitted->length != 2) {
            return false;  // Если разделение не дало две части, возвращаем false
        }

        // Проверяем, существует ли запрашиваемая таблица
        int columnIndex = -1;
        try {
            MyVector<std::string>* colNames = GetMap(jsonStructure, part1Splitted->data[0], clientSocket);  // Получаем имена столбцов таблицы
            for (int i = 0; i < colNames->length; i++) {
                if (colNames->data[i] == part1Splitted->data[1]) {
                    columnIndex = i;  // Находим индекс столбца
                    break;
                }
            }
        } catch (const std::exception& e) {
            //std::cerr << e.what() << ": Tаблица " << part1Splitted->data[0] << " отсутствует" << std::endl;  // Выводим ошибку, если таблица отсутствует
            sendToClient(clientSocket, "Tаблица " + part1Splitted->data[0] + " отсутствует");
            return false;
        }

        if (columnIndex == -1) {
            //std::cerr << "Столбец " << part1Splitted->data[1] << " отсутствует в таблице " << part1Splitted->data[0] << std::endl;  // Выводим ошибку, если столбец отсутствует
            sendToClient(clientSocket, "Столбец " + part1Splitted->data[1] + " отсутствует в таблице " + part1Splitted->data[0]);
            return false;
        }

        std::string delApostr = SanitizeText(node->value.data[2], clientSocket);  // Удаляем апострофы из значения условия
        if (namesOfTable == part1Splitted->data[0] && row.data[columnIndex + 1] == delApostr) {
            return true;  // Если строка удовлетворяет условию, возвращаем true
        }

        return false;  // В противном случае возвращаем false
    }
    case NodeType::OrNode:
        return isValidRow(node->left, row, jsonStructure, namesOfTable, clientSocket) ||  // Проверяем левое поддерево
                isValidRow(node->right, row, jsonStructure, namesOfTable, clientSocket);  // Проверяем правое поддерево
    case NodeType::AndNode:
        return isValidRow(node->left, row, jsonStructure, namesOfTable, clientSocket) &&  // Проверяем левое поддерево
                isValidRow(node->right, row, jsonStructure, namesOfTable, clientSocket);  // Проверяем правое поддерево
    default:
        return false;  // По умолчанию возвращаем false
    }
}

#endif