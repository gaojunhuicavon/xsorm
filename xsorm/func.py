#!/usr/bin/env python
# -*- coding: utf-8 -*-

from xsorm.fields import Column


class AggregateFunction(Column):
    def __init__(self, field):
        self._field = field

    @property
    def full_column_name(self):
        return '%s(`%s`.`%s`)' % (getattr(self, '__wrap__'), self._field.model.__model_option__.table_name, self._field.column)


class Avg(AggregateFunction):
    __wrap__ = 'AVG'


class Count(AggregateFunction):
    __wrap__ = 'COUNT'


class Max(AggregateFunction):
    __wrap__ = 'MAX'


class Min(AggregateFunction):
    __wrap__ = 'MIN'


class Sum(AggregateFunction):
    __wrap__ = 'SUM'


def avg(field):
    return Avg(field)


def count(field):
    return Count(field)


def max_(field):
    return Max(field)


def min_(field):
    return Min(field)


def sum_(field):
    return Sum(field)
