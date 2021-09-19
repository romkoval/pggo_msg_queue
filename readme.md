## Эмулятор обработки очереди сообщений на базе таблицы в postgresql

### Параметры
``` go
Usage of ./pggo_msg_queue:
  -delmsg
        delete message from queue when handled
  -dsn string
        pg connect string (default "postgres://test:test@localhost/test")
  -fill-db int
        fill db with rand data (default 1000)
  -logfile string
        logfile name, use '-' for stdout (default "msg_queue.log")
  -rate float
        continue inserting values with rate per second
  -schema string
        pg schema to create table & indexes (default "schema.sql")
  -workers int
        message queue workers num (default 5)
```

### Таблица очереди
``` sql
create table msg_queue(
    msg_id integer not null,
    queue_type varchar(10) not null, -- type1, type2 ...
    status varchar(1) not null, -- N (new), P (processed)
    order_id integer not null, -- records with same order_id and queue_type have to be handled in correct order, 1,2,3,4...
    time_added timestamp with time zone not null,
    time_processed timestamp with time zone
);
create unique index msg_queue_uniq_msg_id on msg_queue(msg_id);
create index msg_queue_time_added on msg_queue(time_added);
```

### Алгоритм

* Создаем таблицу очереди в PG DB. Схему берем из файла `schema.sql`
* Записываем в таблицу `-fill-db` количество сообщений. Сообщения создаются трех типов и двумя различными `order_id`
* Если указан параметр `-rate`, данные не записываются перед началом обработки, а добавляются с максимальной скоростью `rate` в секунду параллельно с обработкой.
* Запускаются `-workers` обработчиков для трех типов сообщений: `type1, type2, type3`. Очереди обрабатываются конкурентно.
* Программа завершается как только `-fill-db` сообщений будет обработано

* Задержка обработки (эмуляция полезной работы над сообщением): random(1-10) ms

#### Пример

Запущено 3 обработчика очереди Type1.

| msg_id | queue_type | status | order_id |          time_added           | time_processed |
-------- | -----------| -------| ---------| ------------------------------| ---------------- |
|   1 | type1      | N      |        1 | 2021-09-18 23:57:53.48036+03  | |
|   2 | type1      | N      |        2 | 2021-09-18 23:57:53.525959+03 | |
|   3 | type1      | N      |        2 | 2021-09-18 23:57:53.544526+03 | |
|   4 | type1      | N      |        1 | 2021-09-18 23:57:53.584574+03 | |
|   5 | type1      | N      |        1 | 2021-09-18 23:57:53.624685+03 | |

Первые два обработчика успеют взять msg_id 1 и 2, остальные обработчики получат no data found, так как все сообщения с отличающимеся order_id уже в работе.

| handler | msg_id | order_id |
|------|-----|------|
| handler 1 | 1 | 1 |
| handler 2 | 2 | 2 |
| handler 3 | not found | not found |


### Запрос на новое сообщение из очереди

Запрос читается из файла `next_msg.sql`

``` sql
      with next_msg as (
        select msg_id from msg_queue mq
        where
            queue_type = $1
            and status = 'N'
            and not exists (select 1 from msg_queue
                            where
                            queue_type=mq.queue_type
                            and order_id = mq.order_id
                            and status='N'
                            and msg_id != mq.msg_id
                            and time_added <= mq.time_added)
        limit 1
        for update skip locked
     )
     update msg_queue mq
        set status='P'
        from next_msg
        where mq.msg_id = next_msg.msg_id
     returning mq.msg_id, mq.queue_type, mq.order_id
```

## Запуск

### Перед первым запуском нужно выполнить
``` bash
go get
```

### Сборка
``` bash
go build
```

### Запуск

12 обработчиков (по 3 на каждый тип сообщений) обрабатывают очередь, которая пополняется со скоростью 100 сообщений в секунду.

``` bash
./pggo_msg_queue -fill-db 100000 -workers 12 -rate 100 -delmsg
```

### log обработки

Лог в файле `msg_queue.log`


### Плюсы/минусы

* (+) все обработчики максимально заняты, нет необходимости назначать номер обработчика заранее. Очередь обрабатывается равномерно с соблюдением очередности `order_id` и типа очереди `queue_type`
* (-) если обработчиков для одного типа сообщений больше чем количество уникальных order_id, то каждый из них, кому не хватило сообщения из данной группы, получить `not found` и будет спать какое-то время, например, 1 сек.
* (-) для более быстрой обработки нового сообщения, приходится делать короткий интервал сна. Высокий рейт полинга. ***

 *** Можно посылать сигнал в первый (или случайный) обработчик группы при поступлении нового сообщения на обработку, тогда если обработчик не зарят и сообщения с таким типом и order_id не в работе, сообщение будет обработано сразу.
Если обработчик занят, он попробует взять сообщение сразу после завершения обработки предыдущего.
Если обработчик не занят и не смог взять сообщение в работу, так как все сообщения данного типа и order_id в работе, оно будет взято первым освободившимся обработчиком.
