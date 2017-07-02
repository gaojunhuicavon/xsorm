#!/usr/bin/env python
# -*- coding: utf-8 -*-
from xsorm.exception import DefinitionError
from xsorm.fields import Field, ForeignKey


class ModelOption(object):

    def __init__(self):
        self.table_name = None  # 表名
        self.primary_key = None  # 主键的域的信息
        self.fields = []  # 所有域信息
        self.model = None  # model类


def snake_string(origin_string):
    result = []
    for i, s in enumerate(origin_string):
        if s.isupper() and i > 0:
            result.append('_')
        result.append(s.lower())
    return ''.join(result)


def declarative_base():
    class _ModelMetaclass(type):

        __models__ = []
        __table_name_models = {}

        def __new__(mcs, name, bases, attrs):

            # 排除Model类
            if name == 'Model':
                return type.__new__(mcs, name, bases, attrs)

            table_name = attrs.get('__tablename__', snake_string(name))
            if table_name in mcs.__table_name_models:
                raise DefinitionError('The same model has been bind to this base.')

            model = type.__new__(mcs, name, bases, attrs)
            mcs.__models__.append(model)
            mcs.__table_name_models[table_name] = model

            model_option = ModelOption()
            model_option.table_name = table_name
            model_option.model = model

            for k, v in attrs.items():
                if isinstance(v, Field):

                    # 属性名
                    v.field = k

                    # 列名
                    if not v.column:
                        if isinstance(v, ForeignKey):
                            reference_option = v.reference.__model_option__
                            ref_table = reference_option.table_name
                            ref_col = reference_option.primary_key.column
                            v.column = '%s_%s' % (ref_table, ref_col)
                        else:
                            v.column = k

                    # 记录model
                    v.model = model

                    # 主键
                    if v.primary_key:
                        if model_option.primary_key:
                            raise DefinitionError('One model must have only one primary key field')
                        model_option.primary_key = v

                    # 记录在域集中
                    model_option.fields.append(v)

            if not model_option.primary_key:
                raise DefinitionError('No primary key is specified for the model %s' % name)

            setattr(model, '__model_option__', model_option)
            return model

    class Model(dict, metaclass=_ModelMetaclass):

        def __init__(self, *args, **kwargs):
            super(Model, self).__init__(*args, **kwargs)
            # 设置默认值
            for field in self.__model_option__.fields:
                field_name = field.field
                if field_name not in self:
                    if callable(field.default):
                        setattr(self, field_name, field.default())
                    else:
                        setattr(self, field_name, field.default)

        def __getattribute__(self, item):
            try:
                return self[item]
            except KeyError:
                return super(Model, self).__getattribute__(item)

        def __setattr__(self, key, value):
            self[key] = value

    return Model
