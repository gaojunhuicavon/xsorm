#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from functools import reduce

from .exception import NoResultError, JoinError

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
            # 参照表
            table_rel = model.__table_rel__
            ons = []

            # model被参照
            for foreign_key in table_rel[model.__model_option__.table_name]:
                if foreign_key.model in self._models:
                    ons.append(foreign_key == model.__model_option__.primary_key)

            # model参照其他
            for m in self._models:
                for foreign_key in table_rel[m.__model_option__.table_name]:
                    if foreign_key.model is model:
                        ons.append(foreign_key == m.__model_option__.primary_key)

            # 计算出on
            if not ons:
                raise JoinError('No join condition.')
            if len(ons) == 1:
                on = ons[0]
            else:
                on = reduce(lambda x, y: x & y, ons)
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
        mappings = {}
        columns = []
        tables = []
        args = []
        query = []

        # table references
        for model in self._models:
            model_option = model.__model_option__
            if model not in self._join:
                tables.append('`%s`' % model_option.table_name)
            for field in model_option.fields:
                alias = '%s_%s' % (model_option.table_name, field.column)
                columns.append('%s AS `%s`' % (field.full_column_name, alias))
                mappings[alias] = field
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

        results = []
        cache = {}
        for each in self._models:
            cache[each.__model_option__.table_name] = {}

        # 组织数据返回
        for row in self._cursor.fetchall():
            model_objects = []
            cache_ = {}

            # 初始化该行记录对应的对象
            for each in self._models:
                model_object = each()
                cache_[each.__model_option__.table_name] = model_object
                model_objects.append(model_object)
            # 为对象的对应属性赋值
            for alias, field in mappings.items():
                model_object = cache_[field.model.__model_option__.table_name]
                setattr(model_object, field.field, row[alias])
            # 添加到总的缓存中
            for model_object in model_objects:
                model_option = model_object.__model_option__
                table_name = model_option.table_name
                primary = model_object[model_option.primary_key.field]
                cache[table_name][primary] = model_object
            # 添加到结果中
            results.append(model_objects)

        # 添加关联
        for model_objects in results:
            for model_object in model_objects:
                for foreign_key in model_object.__model_option__.foreign_keys:
                    rel_table_name = foreign_key.reference.__model_option__.table_name
                    val = model_object[foreign_key.field]
                    model_object[foreign_key.field] = (cache.get(rel_table_name) or {}).get(val, foreign_key.default)

        return results
