# Имя исполняемого файла
TARGET = Database

# Список исходных файлов .cpp
SRCS = main.cpp

# Список заголовочных файлов .h и .hpp
HDRS = CustomStructures/json.h CRUDOperations/DeleteValue.hpp CRUDOperations/InsertValue.hpp CRUDOperations/SelectValue.hpp Other/Utilities.hpp CRUDOperations/WhereValue.hpp CustomStructures/MyHashMap.hpp CustomStructures/MyVector.hpp CustomStructures/MyHashCode.hpp JsonParser.hpp

# Список объектных файлов .o, которые будут созданы
OBJS = $(SRCS:.cpp=.o)

# Флаги компиляции
CXXFLAGS = -Wall -Wextra -std=c++17

# Компилятор
CXX = g++

# Правило для сборки исполняемого файла
$(TARGET): $(OBJS)
	$(CXX) $(OBJS) -o $(TARGET)

# Правило для компиляции каждого .cpp файла в .o файл
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Правило для очистки объектных файлов и исполняемого файла
clean:
	rm -f $(OBJS) $(TARGET)

# Правило для перекомпиляции всего проекта
rebuild: clean $(TARGET)

# Правило по умолчанию
.PHONY: all clean rebuild