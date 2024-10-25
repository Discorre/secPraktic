#ifndef MYHASHMAP_H
#define MYHASHMAP_H

#include <iostream>
#include <string>

#include "../Other/Utilities.hpp"

// Структура для хранения значения
template <typename TK, typename TV>
struct NodeMap {
    TK key;  // Ключ
    TV value;  // Значение
    NodeMap* next;  // Указатель на следующий узел (для обработки коллизий)
};

// Структура для хранения ключа и значения
template <typename TK, typename TV>
struct MyHashMap {
    NodeMap<TK, TV>** data;  // Массив указателей на узлы
    int length;  // Количество элементов в хэш-таблице
    int capacity;  // Вместимость хэш-таблицы
    int LoadFactor;  // Фактор загрузки (процент заполнения, при котором происходит расширение)
};

// Хэш-функция для ключа типа int
template <typename TK>
int HashCode(const TK& key) {
    unsigned long hash = 5381;  // Начальное значение хэша
    int c = 0;
    for (char ch : key) {
        hash = ((hash << 5) + hash) + ch;  // hash * 33 + c
    }
    return hash;
}

// Инициализация хэш-таблицы
template <typename TK, typename TV>
MyHashMap<TK, TV>* CreateMap(int initCapacity, int initLoadFactor) {
    if (initCapacity <= 0 || initLoadFactor <= 0 || initLoadFactor > 100) {
        throw std::runtime_error("Индекс вне диапазона");  // Выбрасываем ошибку, если параметры некорректны
        //sendToClient3(clientSocket, "Индекс вне диапазона");
    }

    MyHashMap<TK, TV>* map = new MyHashMap<TK, TV>;  // Создаем новую хэш-таблицу
    map->data = new NodeMap<TK, TV>*[initCapacity];  // Выделяем память под массив указателей
    for (int i = 0; i < initCapacity; i++) {
        map->data[i] = nullptr;  // Инициализируем все указатели как nullptr
    }

    map->length = 0;  // Инициализируем количество элементов
    map->capacity = initCapacity;  // Устанавливаем вместимость
    map->LoadFactor = initLoadFactor;  // Устанавливаем фактор загрузки
    return map;
}

// Расширение хэш-таблицы
template <typename TK, typename TV>
void Expansion(MyHashMap<TK, TV>& map) {
    int newCap = map.capacity * 2;  // Новая вместимость в два раза больше текущей
    NodeMap<TK, TV>** newData = new NodeMap<TK, TV>*[newCap];  // Выделяем память под новый массив указателей
    for (int i = 0; i < newCap; i++) {
        newData[i] = nullptr;  // Инициализируем все указатели как nullptr
    }
    // Проход по всем ячейкам
    for (int i = 0; i < map.capacity; i++) {
        NodeMap<TK, TV>* curr = map.data[i];
        // Проход по парам коллизионных значений и обновление
        while (curr != nullptr) {
            NodeMap<TK, TV>* next = curr->next;
            int index = HashCode(curr->key) % newCap;  // Вычисляем новый индекс
            curr->next = newData[index];
            newData[index] = curr;
            curr = next;
        }
    }

    delete[] map.data;  // Освобождаем память старого массива

    map.data = newData;  // Устанавливаем новый массив
    map.capacity = newCap;  // Обновляем вместимость
}

// Обработка коллизий
template <typename TK, typename TV>
void CollisionManage(MyHashMap<TK, TV>& map, int index, const TK& key, const TV& value) {
    NodeMap<TK, TV>* newNode = new NodeMap<TK, TV>{key, value, nullptr};  // Создаем новый узел
    NodeMap<TK, TV>* curr = map.data[index];
    while (curr->next != nullptr) {
        curr = curr->next;  // Ищем последний узел в цепочке
    }
    curr->next = newNode;  // Добавляем новый узел в конец цепочки
}

// Добавление элементов
template <typename TK, typename TV>
void AddMap(MyHashMap<TK, TV>& map, const TK& key, const TV& value) {
    if ((map.length + 1) * 100 / map.capacity >= map.LoadFactor) {
        Expansion(map);  // Если достигнут фактор загрузки, расширяем хэш-таблицу
    }
    int index = HashCode(key) % map.capacity;  // Вычисляем индекс
    NodeMap<TK, TV>* temp = map.data[index];
    if (temp != nullptr) {
        if (temp->key == key) {
            // Обновляем значение ключа
            temp->value = value;
            map.data[index] = temp;
        } else {
            CollisionManage(map, index, key, value);  // Обрабатываем коллизию
        }
    } else {
        NodeMap<TK, TV>* newNode = new NodeMap<TK, TV>{key, value, map.data[index]};  // Создаем новый узел
        map.data[index] = newNode;
        map.length++;  // Увеличиваем количество элементов
    }
}

// Поиск элементов по ключу
template <typename TK, typename TV>
TV GetMap(const MyHashMap<TK, TV>& map, const TK& key, int clientSocket) {
    int index = HashCode(key) % map.capacity;  // Вычисляем индекс
    NodeMap<TK, TV>* curr = map.data[index];
    while (curr != nullptr) {
        if (curr->key == key) {
            return curr->value;  // Возвращаем значение, если ключ найден
        }
        curr = curr->next;
    }

    //throw std::runtime_error("Ключ не найден");  // Выбрасываем ошибку, если ключ не найден
    sendToClient(clientSocket, "Ключ не найден\n");
}

// Удаление элементов
template <typename TK, typename TV>
void DeleteMap(MyHashMap<TK, TV>& map, const TK& key, int clientSocket) {
    int index = HashCode(key) % map.cap;  // Вычисляем индекс
    NodeMap<TK, TV>* curr = map.data[index];
    NodeMap<TK, TV>* prev = nullptr;
    while (curr != nullptr) {
        if (curr->key == key) {
            if (prev == nullptr) {
                map.data[index] = curr->next;  // Удаляем первый элемент в цепочке
            } else {
                prev->next = curr->next;  // Удаляем элемент из середины или конца цепочки
            }
            delete curr;  // Освобождаем память
            map.len--;  // Уменьшаем количество элементов
            return;
        }
        prev = curr;
        curr = curr->next;
    }
    //throw std::runtime_error("Ключ не найден");  // Выбрасываем ошибку, если ключ не найден
    sendToClient(clientSocket, "Ключ не найден\n");
}

// Очистка памяти
template <typename TK, typename TV>
void DestroyMap(MyHashMap<TK, TV>& map) {
    for (int i = 0; i < map.capacity; i++) {
        NodeMap<TK, TV>* curr = map.data[i];
        while (curr != nullptr) {
            NodeMap<TK, TV>* next = curr->next;
            delete curr;  // Освобождаем память каждого узла
            curr = next;
        }
    }
    delete[] map.data;  // Освобождаем память массива указателей
    map.data = nullptr;
    map.length = 0;
    map.capacity = 0;
}

#endif