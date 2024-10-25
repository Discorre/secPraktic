# DatabaseClone

## Просмотр данный (Рабочий пример)
```
SELECT cars.Make cars.Model FROM cars WHERE cars.Year = '2022' AND cars.Price = '42000'
```

## Удаление данных 
```
DELETE FROM cars WHERE cars.Year = '2022'
```

## Добавление данных 
```
INSERT INTO cars VALUES ('Toyota', 'Civic','2024','22000')
```


## Компиляция Database
```
make && rm main.o
```

## Запуск Database
```
./Database
```









Made in China
