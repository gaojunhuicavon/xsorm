#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mysql.connector
import mysql.connector.pooling
from mysql.connector import errorcode
import logging

from xsorm import ForeignKey
from xsorm.query import Query
_logger = logging.getLogger('xsorm')


class SessionMaker:
    def __init__(self, *args, **kwargs):
        self._pool = mysql.connector.pooling.MySQLConnectionPool(*args, **kwargs)

    def __call__(self):
        return Session(self._pool.get_connection())


class Session:
    def __init__(self, connection):
        self._cnx = connection
        self._cursor = self._cnx.cursor()

    def insert(self, model_object):
        """
        :param model_object: 包含设置了主键
        :type model_object: xsorm.model.Model
        :return:
        """
        cursor = self._cursor
        fields = model_object.__fields__
        escaped_fields = model_object.__escaped_fields__
        table_name = model_object.__tablename__
        query = (
            'INSERT INTO `%s`(%s) VALUES (%s)' %
            (table_name, escaped_fields, ', '.join(['%s'] * len(fields)))
        )
        args = []
        for f in fields:
            args.append(model_object[f])
        print(query, args)
        cursor.execute(query, args)
        inserted_id = cursor.lastrowid
        self._cnx.commit()
        return inserted_id

    def insert_many(self, bulk, *model_objects):
        cursor = self._cursor
        if not model_objects:
            return
        model = None
        for i in range(1, len(model_objects)):
            if model_objects[i].__class__ != model_objects[i - 1].__class__:
                raise TypeError('The model objects must be the instance of the same model')
            model = model_objects[i].__class__
        fields = model.__fields__
        escaped_fields = model.__escaped_fields__
        placeholder = ', '.join(['?'] * len(fields))
        for i in range(bulk, len(model_objects), bulk):
            objects = model_objects[i - bulk:i]
            placeholders = ', '.join(['(%s)' % placeholder] * len(objects))
            query = (
                'INSERT INTO `%s`(%s) VALUES %s' %
                (model.__tablename__, escaped_fields, placeholders)
            )
            args = []
            for model_object in objects:
                for field in fields:
                    args.append(model_object[field])
            cursor.execute(query, args)

    def update(self, model_object, *fields):
        cursor = self._cursor
        table_name = model_object.__tablename__
        fields = fields or model_object.__fields__
        mappings = model_object.__mappings__
        primary_key = model_object.__primary_key__
        set_fields = []
        for field in fields:
            set_fields.append('`%s`=?' % mappings[field])
        set_fields = ', '.join(set_fields)
        where = []
        for field in primary_key:
            where.append('`%s`=?' % field)
        query = (
            'UPDATE TABLE `%s` SET %s WHERE %s' %
            (table_name, set_fields, where)
        )
        args = []
        for field in fields:
            args.append(model_object[field])
        for field in primary_key:
            args.append(model_object[field])
        cursor.execute(query, args)
        self._cnx.commit()

    def read(self, model_object):
        cursor = self._cursor
        escaped_fields = model_object.__escaped_fields__
        table_name = model_object.__tablename__
        primary_key = model_object.__primary_key__
        fields = model_object.__fields__
        where = []
        for field in primary_key:
            where.append('`%s`=?' % field)
        where = ' and '.join(where)
        where = where.replace('?', '%s')
        query = (
            'SELECT %s FROM `%s` WHERE %s LIMIT 1' %
            (escaped_fields, table_name, where)
        )
        args = [model_object[f] for f in primary_key]
        cursor.execute(query, args)
        result = next(cursor)
        for field, value in zip(fields, result):
            model_object[field] = value

    def delete(self, model_object):
        """
        删除指定记录
        :param model_object: 已经为主键赋值的Model对象
        :type model_object: xsorm.model.Model
        :return: 影响的行数
        :rtype: int
        """
        cursor = self._cursor
        primary_key = model_object.__primary_key__
        where = ','.join([('`%s`=' % field)+'%s' for field in primary_key])
        query = (
            'DELETE FROM `%s` WHERE %s' %
            (model_object.__tablename__, where)
        )
        args = [model_object[field] for field in primary_key]
        cursor.execute(query, args)
        row_count = cursor.rowcount
        self._cnx.commit()
        return row_count

    def query(self, model, *models):
        return Query(self._cursor, model, *models)

    def raw(self, sql, *args):
        pass

    def close(self):
        self._cursor.close()
        self._cnx.close()

    def create_all(self, base):
        tables = []
        foreign_keys = []
        for model in base.__models__:
            columns = []
            option = model.__model_option__
            table_name = option.table_name
            for field in option.fields:
                # 通用处理
                ddl = ['`%s`' % field.column, field.__ddl__]
                if field.primary_key:
                    ddl.append('PRIMARY KEY')
                if field.auto_increment:
                    ddl.append('AUTO_INCREMENT')
                if field.unsigned:
                    ddl.append('UNSIGNED')
                if not field.nullable:
                    ddl.append('NOT NULL')
                columns.append('    ' + ' '.join(ddl))

                # 处理外键
                if isinstance(field, ForeignKey):
                    reference_option = field.reference.__model_option__
                    ref_table = reference_option.table_name
                    ref_column = reference_option.primary_key.column

                    foreign_keys.append(
                        'ALTER TABLE `%s` ADD FOREIGN KEY (%s) '
                        'REFERENCES %s(%s) ON DELETE %s ON UPDATE %s' %
                        (
                            table_name, field.column,
                            ref_table, ref_column, field.on_update, field.on_delete
                        )
                    )
            columns = ',\n'.join(columns)
            tables.append('CREATE TABLE `%s` (\n%s\n)' % (table_name, columns))
        for table in tables:
            _logger.debug(table)
            try:
                self._cursor.execute(table)
            except mysql.connector.Error as err:
                if err.errno != errorcode.ER_TABLE_EXISTS_ERROR:
                    raise
        for foreign_key in foreign_keys:
            _logger.debug(foreign_key)
            self._cursor.execute(foreign_key)

    def drop_all(self, base):
        off_foreign_key = 'SET foreign_key_checks = 0'
        on_foreign_key = 'SET foreign_key_checks = 1'
        _logger.debug(off_foreign_key)
        self._cursor.execute(off_foreign_key)
        try:
            for model in base.__models__:
                try:
                    drop_table = 'DROP TABLE %s' % model.__model_option__.table_name
                    _logger.debug(drop_table)
                    self._cursor.execute(drop_table)
                except mysql.connector.Error as err:
                    if err.errno != errorcode.ER_BAD_TABLE_ERROR:
                        raise

        finally:
            _logger.debug(on_foreign_key)
            self._cursor.execute(on_foreign_key)
