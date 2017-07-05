#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mysql.connector
import mysql.connector.pooling
from mysql.connector import errorcode
import logging
from collections import defaultdict

from . import ForeignKey
from .fields import CASCADE
from .exception import NoResultError
from .query import Query
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
        self._closed = False

    def insert(self, model_object):
        """
        利用Model对象在对应的表插入一行记录，没有主动赋值的列将使用default参数设置的默认值，如果default参数为callable，则为生成对象时的
        执行结果，如果否则就是default的值，default默认为None，及数据库中的NULL，设置为AUTO_INCREMENT的列不会在SQL中指定他的值而是让数
        据库自动生成。
        例如：
        class User(Base):
            id = IntField(primary_key=True, auto_increment=True)
            age = IntField()
            height = IntField(default=lambda: int(time.time()))
            weight = IntField()
        user = User(id=1, age=3)
        row_id = session.insert(user)
        print(row_id == user.id)  # True
        等同于执行了SQL：
        INSERT INTO `user`(`age`, `height`, `weight`) VALUES(3, 1498935425, None)
        插入成功，如果这个表的有AUTO_INCREMENT的列，将返回这条记录的该列的值，也就是row_id，并且会将该值自动赋值给该对象对应的属性，请参
        考上面代码中的判断结果。
        :param model_object: 作为记录的Model对象
        :return: row_id
        """
        cursor = self._cursor
        option = model_object.__model_option__
        primary = option.primary_key
        fields = []
        args = []
        for field in option.fields:
            if field.auto_increment:
                continue
            args.append(model_object[field.field])
            fields.append('`%s`' % field.column)

        query = 'INSERT INTO `%s`(%s) VALUES(%s)' % (option.table_name, ', '.join(fields), ', '.join('?' * len(fields)))
        query = query.replace('?', '%s')
        cursor.execute(query, args)
        _logger.debug(cursor.statement)
        inserted_id = cursor.lastrowid
        self._cnx.commit()
        if primary.auto_increment:
            model_object[primary.field] = inserted_id
        return inserted_id

    def insert_many(self, bulk, *model_objects):
        raise NotImplementedError

    def update(self, model_object, *update_fields):
        """
        利用Model对象更新数据库记录，该对象必须设置了主键的值，默认情况下会更新该model的所有字段，可以设置update_fields参数来指定需要更新
        的字段，执行更新后会返回被更新的记录数
        例如：
        class User(Base):
            id = IntField(primary_key=True, auto_increment=True)
            age = IntField()
            height = IntField()
        user = session.query(User).filter(User.id >= 5).one()  # {id: 5, age: 12, height: 165}
        user.age += 1
        user.height += 12
        affected_row = session.update(user)  # UPDATE `user` SET `id` = 5, `age` = 13, `height` = 177 WHERE `id` = 5
        print(affected_row)  # 1
        user.age = 1234
        affected_row = session.update(user, User.age)  # UPDATE `user` SET `age` = 1234 WHERE `id` = 5
        print(affected_row)  # 1
        affected_row = session.update(user, User.age)
        print(affected_row)  # 0, 因为该记录没有变化
        :param model_object: 设置了主键的Model对象
        :return: UPDATE语句影响的行数
        """
        cursor = self._cursor
        option = model_object.__model_option__
        primary = option.primary_key
        set_fields = []
        args = []
        for field in update_fields or option.fields:
            set_fields.append('`%s` = ?' % field.column)
            args.append(model_object[field.field])
        args.append(model_object[primary.field])
        query = (
            'UPDATE `%s` SET %s WHERE `%s` = ?' %
            (option.table_name, ', '.join(set_fields), primary.column)
        )
        query = query.replace('?', '%s')
        cursor.execute(query, args)
        _logger.debug(cursor.statement)
        affected_rows = cursor.rowcount
        self._cnx.commit()
        return affected_rows

    def read(self, model_object, raise_=False):
        """
        获取主键和model_object中的主键相同的行的记录
        例如：
        class User(Base):
            id = IntField(primary_key=True, auto_increment=True)
            age = IntField()
        # SELECT `id`, `age` FROM `user` WHERE `id` = 5
        session.read(User(id=5))  # {'id': 5, 'age': 12}
        # SELECT `id`, `age` FROM `user` WHERE `id` = 1
        session.read(User(id=-1)  # None
        session.read(User(id=-1, True)  # NoResultError
        :param model_object: 主键赋值了的model object
        :param raise_: 若为True，则当没有匹配的记录返回时抛出异常NoResultError
        :return: 一条记录
        :rtype: Model
        """
        option = model_object.__model_option__
        model = option.model
        primary = option.primary_key
        return self.query(model).filter(primary == model_object[primary.field]).one(raise_)

    def delete(self, model_object):
        delete_sqls, args = self._delete(model_object)
        affected_row = 0
        for each in self._cursor.execute(delete_sqls, args, multi=True):
            affected_row += each.rowcount
            _logger.debug(each.statement)
        self._cnx.commit()
        return affected_row

    def _delete(self, model_object, related_dict=None):
        option = model_object.__model_option__
        primary = option.primary_key
        pri_field = primary.field
        foreign_keys = model_object.__table_rel__[option.table_name]
        delete_sqls = []
        args = []
        if related_dict is None:
            related_dict = defaultdict(set)
        related_objects = []

        # 查找所有一级关联的记录
        for foreign_key in foreign_keys:
            if foreign_key.on_delete != CASCADE:
                continue
            try:
                related_objects_ = self.query(foreign_key.model).filter(foreign_key == model_object[pri_field]).all()
                related_objects.extend(related_objects_)
            except NoResultError:
                pass

        # 删除对所有一级关联的记录
        for related_object in related_objects:
            model_option = related_object.__model_option__
            table_name = model_option.table_name
            primary_value = related_object[model_option.primary_key.field]
            if primary_value not in related_dict[table_name]:
                related_dict[table_name].add(primary_value)
                delete_sqls_, args_ = self._delete(related_object, related_dict)
                delete_sqls.append(delete_sqls_)
                args.extend(args_)

        # 删除目标记录
        query = 'DELETE FROM `%s` WHERE `%s` = ?' % (option.table_name, primary.column)
        query = query.replace('?', '%s')
        delete_sqls.append(query)
        args.append(model_object[primary.field])
        return ';'.join(delete_sqls), args

    def query(self, model, *models):
        return Query(self._cursor, model, *models)

    def raw(self, sql, *args):
        raise NotImplementedError

    def close(self):
        """
        关闭Session，关闭后该session不能执行任何其他功能，因为该session所持有的数据库连接已经重新投入到连接池中，提供给其他session使用，
        不必要每次手动执行close方法，因为每当session脱离了其作用域被gc时，就会自动执行close方法，但是也可以手动执行，用于提前关闭session，
        从而提前将连接重新投入到连接池
        """
        if self._closed:
            return
        self._cursor.close()
        self._cnx.close()
        self._closed = True

    def __del__(self):
        self.close()

    @property
    def closed(self):
        return self._closed

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
