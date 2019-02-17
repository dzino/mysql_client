#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql.cursors              # подключение к БД
from argparse import ArgumentParser # флаги с параметрами
from tabulate import tabulate       # печать в терминал таблиц

class mysql:

    def __init__(self):

        """
        : param connection  : объект соединения, создается автоматически
        : type  connection  : obj
        : param write       : режим печати в терминал
        : type  write       : str
        : param table       : таблица БД. '-tt' - показать все таблицы
        : type  table       : str
        : param target      : ключи массива вывода, которые нужно выделить
        : type  target      : array
        : param sql         : произвольный sql-запрос
        : type  sql         : str
        : param result      : результат обработки метода
        : type  result      : array
        : param delete      : ids для удаления строк 
        : type  delete      : str
        : param build       : данные для добавления строчки
        : type  build       : str
        : param update      : данные для обновления строчки
        : type  update      : str
        : param clear       : очистить таблицу
        : type  clear       : bool
        : param two         : сделать два снимка таблиц и сравнить
        : type  two         : bool
        : method parameters : чтение параметров из командно строки
        : method connect    : подключение
        : method request    : прямой sql-запрос
        : method get_tables : получить список таблиц
        : method get_lines  : получить строки таблицы
        : method add_line   : добавить запись в таблицу
        """

        self.host     = ''
        self.port     = 3306
        self.user     = ''
        self.password = ''
        self.db       = ''

        self.connection = ""
        self.write      = "ln"
        self.table      = ""
        self.target     = {}
        self.sql        = ""
        self.build      = ""
        self.update     = ""
        self.result     = []
        self.delete     = ""
        self.clear      = False
        self.two        = False

        self.parameters()
        if self.table:
            self.connect()

        if self.sql != ""                     : self.request()
        elif self.table and self.build != ""  : self.add_line()
        elif self.table and self.update != "" : self.up_line()
        elif self.table and self.delete != "" : self.del_line()
        elif self.table == 't'                : self.get_tables()
        elif self.table and self.clear        : self.clear_table()
        elif self.table                       : self.get_lines()
        else: print('''\33[92mУкажите таблицу -t\n\33[90mПримеры:\33[0m
python3 __init__.py -s "SHOW TABLES"
python3 __init__.py -tt
python3 __init__.py -t users -wt
python3 __init__.py -t orders -b id::35//name::Tom
python3 __init__.py -t orders -u 37//name::markkk
python3 __init__.py -t orders -d 34,35,36
python3 __init__.py -t orders -c
python3 __init__.py -tt -tw -wt
''')

    def parameters(self):

        """ установка параметров в файле """

        parser = ArgumentParser(prog='python3 mysql', description='''
            Клиент управлиния записями БД. Примеры: \33[90mpython3 __init__.py\33[0m
        ''')
        parser.add_argument("-t", "--table",  help ="\33[92m|\33[90m таблица БД. '-tt' - показать все таблицы\33[0m")
        parser.add_argument("-tw", "--two",   help ="\33[92m|\33[90m таблица БД сделать два снимка и подсветить разницу\33[0m", action='store_true')
        parser.add_argument("-w", "--write",  help ="\33[92m|\33[90m l - распечатать строчками, t - таблицей\33[0m", choices=['l', 't'])
        parser.add_argument("-s", "--sql",    help ="\33[92m|\33[90m прямой ввод sql-запроса\33[0m")
        parser.add_argument("-b", "--build",  help ="\33[92m|\33[90m создать запись(-t <table>) | <col>::<val>//<col>::<val> (первое значение Уникальное!)\33[0m")
        parser.add_argument("-u", "--update", help ="\33[92m|\33[90m обновить запись(-t <table>) | <id>//<col>::<val>//<col>::<val>\33[0m")
        parser.add_argument("-d", "--delete", help ="\33[92m|\33[90m удалить запись(-t <table>) | <id>,<id>,<id>\33[0m")
        parser.add_argument("-c", "--clear",  help ="\33[92m|\33[90m очистить таблицу [bool]\33[0m", action='store_true')
        args = parser.parse_args()
        if args.write  : self.write  = args.write
        if args.sql    : self.sql    = args.sql
        if args.table  : self.table  = args.table
        if args.build  : self.build  = args.build
        if args.update : self.update = args.update
        if args.delete : self.delete = args.delete
        if args.clear  : self.clear  = True
        if args.two    : self.two    = True

    def connect(self):

        """ соединение с БД """

        self.connection = pymysql.connect(
            host=self.host, port=self.port, user=self.user, password=self.password, db=self.db, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
        )

    def add_line(self):

        """ добавить запись """

        # разбивка введеного параметра
        params       = self.build.split('//')
        columns      = ""
        value        = ""
        first_column = ""
        first_value  = ""

        first = True
        for unit in params:
            param    = unit.split('::')
            columns += ', '  + param[0]
            value   += ', "' + param[1] + '"'
            if first == True:
                first_column = param[0]
                first_value  = '"' + param[1] + '"'
            first = False

        try:
            # добавление записи
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO %s (%s) VALUES (%s);" % (self.table, columns[2:], value[2:])
                cursor.execute(sql)
            self.connection.commit()

            # получаем id...
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM `%s` WHERE `%s`=%s" % (self.table, first_column, first_value)
                cursor.execute(sql)
                self.result = [cursor.fetchone()]
                self.print_arr()
        finally:
            self.connection.close()

    def up_line(self):

        """ добавить запись """

        # разбивка введеного параметра
        params        = self.update.split('//')
        id_line       = params[0]
        del params[0]
        column_value  = ""

        for unit in params:
            param         = unit.split('::')
            column_value += ', ' + param[0] + '="' + param[1] + '"'

        try:
            # добавление записи
            with self.connection.cursor() as cursor:
                sql = 'UPDATE %s SET %s WHERE id="%s";' % (self.table, column_value[2:], id_line)
                cursor.execute(sql)
            self.connection.commit()

            # получаем id...
            with self.connection.cursor() as cursor:
                sql = 'SELECT * FROM `%s` WHERE `id`="%s";' % (self.table, id_line)
                cursor.execute(sql)
                self.result = [cursor.fetchone()]
                self.print_arr()
        finally:
            self.connection.close()

    def del_line(self):

        """ удалить записи """

        cursor = self.connection.cursor()           # соединиться

        # разбивка введеного параметра
        params = self.delete.split(',')
        ids    = ''
        for unit in params:
            ids += ",'" + unit + "'"

        sql = 'DELETE FROM `%s` WHERE id IN (%s);' % (self.table, ids[1:])
        cursor.execute(sql)
        self.connection.commit()

        self.connection.close()                     # разъединиться

    def clear_table(self):

        """ очистить таблицу """

        cursor = self.connection.cursor()           # соединиться

        sql    = 'TRUNCATE TABLE `%s`;' % (self.table)
        cursor.execute(sql)
        self.connection.commit()

        self.connection.close()                     # разъединиться

    def get_tables(self):

        """ получить данные по таблицам (сравнить данные) """

        self.get_tables_once()

        if self.two == True:
            once = self.result
            input('\33[92m>\33[0m [Enter] \33[90mсделать второй снимок\33[0m')
            self.connect()
            self.get_tables_once()
            self.compare_quantity_table(once)
            self.print_arr()
        else:
            self.print_arr()

    def get_tables_once(self):

        """ получить список таблиц с кол-во записей в каждой """

        cursor = self.connection.cursor() # соединиться

        # получить список таблиц
        sql    = "SHOW TABLES"
        cursor.execute(sql)
        tables = cursor.fetchall()

        # получаем колво записей в таблицах
        for line in range(len(tables)):
            for key in tables[line]:
                # получить колво записей (union)
                sql         = "SELECT COUNT(*) AS quantity FROM %s" % (tables[line][key])
                cursor.execute(sql)
                self.result = cursor.fetchall()
                quantity    = self.result[0]['quantity']

                # получаем ВСЕ записи таблицы и выбираем не нулевые столбцы с отсутствующими значениями поумолчанию
                columns = ''
                sql     = "SHOW COLUMNS FROM %s" % (tables[line][key])
                cursor.execute(sql)
                self.result = cursor.fetchall()
                for column in self.result:
                    if column['Null'] == 'NO' and column['Default'] == None:
                        columns += '\33[92m' + column['Field'] + '\33[0m '
                    else:
                        columns += column['Field'] + ' '

            tables[line]['table']   = tables[line][key]  # переименовать столбец таблицы
            del tables[line][key]
            tables[line]['count']   = quantity
            tables[line]['columns'] = columns

        self.result = tables
        self.connection.close() # разъединиться

    def compare_quantity_table(self, once):

        """ сравнить число записей в таблицах """

        # подготовить массив к сравнению
        comparison = {}
        for table in once:
            comparison[table['table']] = table['count']

        for table in range(len(self.result)):
            if  self.result[table]['count'] != comparison[self.result[table]['table']]:
                self.result[table]['count'] = "\33[92m%s\33[0m" % (self.result[table]['count'])

    def get_lines(self):

        """ получить данные таблицы: колво и перые 10 записей """

        cursor = self.connection.cursor() # соединиться

        # получить колво записей
        sql = "SELECT COUNT(*) AS quantity FROM %s" % (self.table)
        cursor.execute(sql)
        self.result = cursor.fetchall()
        quantity    = self.result[0]['quantity']

        # получаем ВСЕ записи таблицы и выбираем не нулевые столбцы с отсутствующими значениями поумолчанию
        target = ''
        sql    = "SHOW COLUMNS FROM %s" % (self.table)
        cursor.execute(sql)
        self.result = cursor.fetchall()
        for line in self.result:
            if line['Null'] == 'NO' and line['Default'] == None:
                self.target[line['Field']] = True
                target += ' ' + line['Field']

        # статус строка
        print('Таблица: \33[92m%s\33[0m    Колво: \33[92m%s\33[0m    Обязательные поля: \33[92m%s\33[0m\n' % (self.table, quantity, target))

        # получаем ВСЕ записи таблицы
        sql = "SELECT * FROM %s LIMIT 10" % (self.table)
        cursor.execute(sql)
        self.result = cursor.fetchall()
        self.print_arr()

        self.connection.close()           # разъединиться

    def request(self):

        """ sql запрос, прямой"""

        cursor = self.connection.cursor() # соединиться

        # получить колво записей (union)
        sql = "%s" % (self.sql)
        cursor.execute(sql)
        self.result = cursor.fetchall()
        self.print_arr()

        self.connection.close()           # разъединиться

    def print_arr(self):

        """ печать: в строчку, столбиком, таблицей """

        headers       = []
        write_headers = True
        matrix_table  = []

        for line in self.result:
            matrix_line = []
            if self.write != 't':
                print('\33[91m*\33[0m', end=' ', flush=True)

            for key in line:

                color = '\33[90m'
                color2 = ''
                if key in self.target:
                    color = '\33[92m'
                if line[key] == None:
                    color2 = '\33[90m'

                if self.write == 't':
                    headers.append('%s%s:\33[0m' % (color, key))
                    matrix_line.append('%s%s\33[0m' % (color2, line[key]))
                elif self.write == 'l':
                    print('%s%s:\33[0m %s%s\33[0m' % (color, key, color2, line[key]), end=' ', flush=True)
                else:
                    print('%s%s:\33[0m %s%s\33[0m' % (color, key, color2, line[key]))

            write_headers = False
            if self.write != 't':
                print('')
            else:
                matrix_table.append(matrix_line)

        if self.write == 't':
            print (tabulate(matrix_table, headers=headers))


if __name__ == '__main__':
    obj = mysql()
