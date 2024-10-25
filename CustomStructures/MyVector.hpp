#ifndef MYVECTOR_H
#define MYVECTOR_H

#include <iostream>
#include <iomanip>

template <typename T>
struct MyVector {
    T* data;      // Массив
    int length;        // Длина
    int capacity;        // Вместимость (capacity)
    int LoadFactor; // Процент заполнения, при котором увеличиваем объем (например, 50%)
};

// Перегрузка оператора вывода
template <typename T>
std::ostream& operator << (std::ostream& os, const MyVector<T>& vector) {
    for (int i = 0; i < vector.length; i++) {
        std::cout << vector.data[i];
        if (i < vector.length - 1) std::cout << std::setw(25);  // Устанавливаем ширину для вывода
    }
    return os;
}

// Инициализация вектора
template <typename T>
MyVector<T>* CreateVector(int initCapacity, int initLoadFactor) {
    if (initCapacity <= 0 || initLoadFactor <= 0 || initLoadFactor > 100) {
        throw std::runtime_error("Индекс вне диапазона");  // Выбрасываем ошибку, если параметры некорректны
    }

    MyVector<T>* vector = new MyVector<T>;  // Создаем новый вектор
    vector->data = new T[initCapacity];  // Выделяем память под массив
    vector->length = 0;  // Инициализируем длину
    vector->capacity = initCapacity;  // Устанавливаем вместимость
    vector->LoadFactor = initLoadFactor;  // Устанавливаем фактор загрузки
    return vector;
}

// Увеличение массива
template <typename T>
void Expansion(MyVector<T>& vector) {
    int newCap = vector.capacity * 2;  // Новая вместимость в два раза больше текущей
    T* newData = new T[newCap];  // Выделяем память под новый массив
    for (int i = 0; i < vector.length; i++) {     // Копируем данные из старого массива в новый
        newData[i] = vector.data[i];
    }
    delete[] vector.data;                      // Очищаем память старого массива
    vector.data = newData;
    vector.capacity = newCap;
}

// Добавление элемента в вектор
template <typename T>
void AddVector(MyVector<T>& vector, T value) {
    if ((vector.length + 1) * 100 / vector.capacity >= vector.LoadFactor) { // Если достигнут фактор загрузки, увеличиваем массив
        Expansion(vector);
    }
    vector.data[vector.length] = value;  // Добавляем элемент
    vector.length++;  // Увеличиваем длину
}

// Удаление элемента из вектора
template <typename T>
void DeleteVector(MyVector<T>& vector, int index) {
    if (index < 0 || index >= vector.length) {
        throw std::runtime_error("Индекс вне диапазона");  // Выбрасываем ошибку, если индекс некорректен
    }

    for (int i = index; i < vector.length - 1; i++) {
        vector.data[i] = vector.data[i + 1];  // Сдвигаем элементы влево
    }

    vector.length--;  // Уменьшаем длину
}

// Замена элемента по индексу
template <typename T>
void ReplaceVector(MyVector<T>& vector, int index, T value) {
    if (index < 0 || index >= vector.length) {
        throw std::runtime_error("Индекс вне диапазона");  // Выбрасываем ошибку, если индекс некорректен
    }
    vector.data[index] = value;  // Заменяем элемент
}

#endif