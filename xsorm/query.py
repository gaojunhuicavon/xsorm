#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from .exception import NoResultError, JoinError
from .fields import ForeignKey

_logger = logging.getLogger('xsorm')


class Query:
    def __init__(self, cursor, *models):
        self._cursor = cursor
        self._models = models
        self._wheres = []
        self._args = []
        self._join = []
        self._on = []
        self._having = []
        self._having_args = []
        self._limit = None
        self._offset = None
        self._order_by = None
        self._group_by = None

    def join(self, model, on=None):
        self._join.append(model)
        if on is not None:
            self._on.append(on)
        else:
            for model_ in self._models:
                for field in model_.__model_option__.fields:
                    if isinstance(field, ForeignKey) and field.reference == model:
                        on_ = model_.__model_option__.primary_key == model.__model_option__.primary_key
                        on = on_ if on is None else on & on_
            if on is None:
                raise JoinError('No join condition.')
            self._on.append(on)
        return self

    def filter(self, operation):
        self._wheres.append(operation.sql)
        self._args.extend(operation.args)
        return self

    def limit(self, limit):
        self._limit = limit
        return self

    def offset(self, offset):
        self._offset = offset
        return self

    def order_by(self, *columns):
        self._order_by = [column.full_column_name for column in columns]
        return self

    def group_by(self, *columns):
        self._group_by = [column.full_column_name for column in columns]
        return self

    def having(self, *operations):
        for operation in operations:
            self._having.append(operation.sql)
            self._having_args.extend(operation.args)
        return self

    def one(self, raise_=False):
        self.limit(1)
        for each in self.all():
            return each

        if raise_:
            raise NoResultError

    def all(self):
        columns = []
        tables = []
        args = []
        query = []

        # table references
        for model in self._models:
            model_option = model.__model_option__
            tables.append('`%s`' % model_option.table_name)
            for field in model_option.fields:
                columns.append(field.full_column_name)
        table_references = ('(%s)' if len(tables) > 1 and self._join else '%s') % ', '.join(tables)
        for join, on in zip(self._join, self._on):
            table_references += ' JOIN `%s` ON %s' % (join.__model_option__.table_name, on.sql)
            args.extend(on.args)

        # 生成query语句和args参数
        query.append('SELECT ' + ', '.join(columns))
        query.append('FROM ' + table_references)
        if self._wheres:
            query.append('WHERE ' + (' AND '.join(self._wheres)))
            args.extend(self._args)
        if self._group_by:
            query.append('GROUP BY ' + ','.join(self._group_by))
        if self._having:
            query.append('HAVING ' + ' AND '.join(self._having))
            args.extend(self._having_args)
        if self._order_by:
            query.append('ORDER BY ' + ','.join(self._order_by))
        if self._limit is not None:
            query.append('LIMIT ?')
            args.append(self._limit)
        if self._offset is not None:
            query.append('OFFSET ?')
            args.append(self._offset)

        # 执行查询
        query = '\n'.join(query).replace('?', '%s')
        self._cursor.execute(query, args)
        _logger.debug(self._cursor.statement)

        # 组织数据返回
        for row in self._cursor.fetchall():
            model_objects = []
            for model in self._models:
                model_option = model.__model_option__
                model_object = model()
                for column_data, field in zip(row, model_option.fields):
                    setattr(model_object, field.field, column_data)
                model_objects.append(model_object)
            yield model_objects[0] if len(model_objects) == 1 else model_objects
