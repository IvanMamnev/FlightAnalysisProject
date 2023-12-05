# Анализ полетов


## Описание данных


- YEAR  
- MONTH 
- DAY
- DAY_OF_WEEK - день недели (числовое значение; 1 - означает понедельник)
- AIRLINE - идентификатор авиалиний (ИАТА код), соответствует полю IATA_CODE из airlines.csv
- FLIGHT_NUMBER - номер рейса
- TAIL_NUMBER - идентификатор самолета
- ORIGIN_AIRPORT- аэропорт отправления 
- DESTINATION_AIRPORT - аэропорт назначения
- SCHEDULED_DEPARTURE - запланированное время вылета
- DEPARTURE_TIME - время отправления
- DEPARTURE_DELAY - время (минуты), на которое рейс был задержан: положительное число означает задержку, отрицительное, наоборот, говорит о том, что вылет произошел раньше
- TAXI_OUT - время от начала движения по взлётно-посадочной полосе до отрыва от неё
- WHEELS_OFF - время, когда самолет отрывается от земли
- SCHEDULED_TIME -  предполагаемое время нахождения в пути
- ELAPSED_TIME - сумма значений AIR_TIME+TAXI_IN+TAXI_OUT
- AIR_TIME - время между wheels_off и wheels_on time
- DISTANCE - расстояние между аэропортами
- WHEELS_ON - время,  когда колеса самолета коснулись взлетной полосы
- TAXI_IN - время от момента приземления на взлетно-посадочную полосу до полной остановки
- SCHEDULED_ARRIVAL - запланированное время прибытия
- ARRIVAL_TIME - время прибытия, рассчитывается как WHEELS_ON+TAXI_IN
- ARRIVAL_DELAY - задержка прибытия (минуты), расчитывается как ARRIVAL_TIME-SCHEDULED_ARRIVAL (отрицательное значение говорит о том, что прибыл раньше времени)
- DIVERTED - самолет приземлился в аэропорту не по расписанию (значение 1 или 0)
- CANCELLED - отмена рейса: 1 - отменен, 0 - без отмены
- CANCELLATION_REASON - причина отмены рейса: A - Авиакомпания/Перевозчик; B - Погода; C - причины, связанные с организацией воздушного движения (работа аэропорта, интенсивное движение в небе и тд); D - Безопасность
- AIR_SYSTEM_DELAY - показывает,  на сколько минут рейс был задержан по причинам, связанным с организацией воздушного движения
- SECURITY_DELAY - показывает,  на сколько минут рейс был задержан по причине безопасности
- AIRLINE_DELAY - показывает,  на сколько минут рейс был задержан по вине авиакомпании
- LATE_AIRCRAFT_DELAY -  показывает,  на сколько минут рейс был задержан по причине задержки предыдущего рейса
- WEATHER_DELAY - показывает,  на сколько минут рейс был задержан в связи с погодными условиями
 

## Показатели для расчета



- топ-10 самых популярных аэропортов по количеству совершаемых полетов
- топ-10 авиакомпаний, вовремя выполняющих рейсы
- топ-10 перевозчиков и аэропортов назначения для каждого аэропорта на основании вовремя совершенного вылета 
- дни недели в порядке своевременности прибытия рейсов, совершаемых в эти дни
- количество рейсов, задержанных по причине  AIR_SYSTEM_DELAY / SECURITY_DELAY / AIRLINE_DELAY / LATE_AIRCRAFT_DELAY / WEATHER_DELAY
- процент от общего количества минут задержки рейсов для  каждой причины (AIR_SYSTEM_DELAY / SECURITY_DELAY / AIRLINE_DELAY / LATE_AIRCRAFT_DELAY / WEATHER_DELAY)


В meta_ifo.csv отражается информация: 

- collected - информация о том, какие данные т.е. за какой период - были проанализированы
- processed - дата, когда произведен анализ


## Дополнительная инофрмация 


Период расчета данных - 1 год. 


В рамках периода данные могут поступать разными частями. Например, сначала за первые 2 месяца, затем за остальные 10 месяцев.

 
Промежуточные расчеты накапливаются, чтобы не пересчитывать всю статистику заново. Итоговые метрики пересчитываются с учетом послупдения новых данных.


Количество колонок, порядок, тип данных, которые поступают на вход, остаются неизменными. Наименование колонок во входящем файле могут меняться. 


Приложение имеет возможность запускаться как локально, так и на кластере. Для запуска на кластере указывать параметр (--class com.example.FlightAnalyzer).
 
