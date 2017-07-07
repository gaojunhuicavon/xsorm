#!/usr/bin/env python
# -*- coding: utf-8 -*-
from abc import ABCMeta
from xsorm.column import Column
CASCADE = 'CASCADE'
NO_ACTION = 'NO ACTION'
SET_NULL = 'SET NULL'
SET_DEFAULT = 'SET DEFAULT'


class Field(Column, metaclass=ABCMeta):

    def __init__(self, nullable=False, primary_key=False, default=None, column=None, on_delete=None,
                 on_update=None, auto_increment=False, unsigned=False):
        self.primary_key = primary_key
        self.default = default
        self.nullable = nullable
        self.column = column
        self.on_delete = on_delete
        self.on_update = on_update
        self.auto_increment = auto_increment
        self.unsigned = unsigned

        self.model = None
        self.field = None

        if not isinstance(self, _IntegerField):
            self.auto_increment = False


class ColumnField(Field, Column):
    @property
    def full_column_name(self):
        return '`%s`.`%s`' % (self.model.__model_option__.table_name, self.column)


class _IntegerField(ColumnField):

    def __init__(self, primary_key=False, auto_increment=False, **kwargs):
        super(_IntegerField, self).__init__(primary_key=primary_key, **kwargs)
        self.auto_increment = auto_increment


class _FloatField(ColumnField):
    pass


class IntField(_IntegerField):
    __ddl__ = 'INT'


class TinyintField(_IntegerField):
    __ddl__ = 'TINYINT'


class SmallIntField(_IntegerField):
    __ddl__ = 'SMALLINT'


class BigIntField(_IntegerField):
    __ddl__ = 'BIGINT'


class FloatField(_FloatField):
    __ddl__ = 'FLOAT'


class DoubleField(_FloatField):
    __ddl__ = 'DOUBLE'


class DecimalField(ColumnField):
    def __init__(self, m=10, d=0, **kwargs):
        super(DecimalField, self).__init__(**kwargs)
        self.__ddl__ = 'DECIMAL(%d,%d)' % (m, d)


NumericField = DecimalField


class BitField(ColumnField):

    def __init__(self, m, **kwargs):
        super(BitField, self).__init__(**kwargs)
        self.__ddl__ = 'BIT(%d)' % m


class DateField(ColumnField):
    __ddl__ = 'DATE'


class DatetimeField(ColumnField):
    __ddl__ = 'DATETIME'


class TimeField(ColumnField):
    __ddl__ = 'TIME'


class YearField(ColumnField):
    __ddl__ = 'YEAR'


class TimestampField(ColumnField):
    __ddl__ = 'TIMESTAMP'


class CharField(ColumnField):

    def __init__(self, length, **kwargs):
        super(CharField, self).__init__(**kwargs)
        self.__ddl__ = 'CHAR(%d)' % length


class VarcharField(ColumnField):
    def __init__(self, length, **kwargs):
        super(VarcharField, self).__init__(**kwargs)
        self.__ddl__ = 'VARCHAR(%d)' % length


class ForeignKey(ColumnField):
    def __init__(self, reference, on_delete=NO_ACTION, on_update=NO_ACTION, **kwargs):
        super(ForeignKey, self).__init__(on_delete=on_delete, on_update=on_update, **kwargs)
        self.reference = reference
        self.__ddl__ = reference.__model_option__.primary_key.__ddl__
