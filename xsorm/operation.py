#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractproperty


class Operation(metaclass=ABCMeta):

    def __or__(self, other):
        if not isinstance(other, Operation):
            raise TypeError('%s is not an operation' % other)
        return Or(self, other)

    def __and__(self, other):
        if not isinstance(other, Operation):
            raise TypeError('%s is not an operation' % other)
        return And(self, other)

    @abstractproperty
    def sql(self):
        pass

    @abstractproperty
    def args(self):
        pass


class BinaryOperation(Operation):

    def __init__(self, left, right):
        self._left = left
        self._right = right

    @property
    def left(self):
        return self._left

    @property
    def right(self):
        return self._right

    @property
    def operator(self):
        return getattr(self, '__operator__')

    @property
    def sql(self):
        return '%s %s %s' % \
               (self._param_to_sql(self.left), getattr(self, '__operator__'), self._param_to_sql(self.right))

    @property
    def args(self):
        return self._param_to_args(self.left) + self._param_to_args(self.right)

    @staticmethod
    def _param_to_sql(param):
        from .column import Column
        if isinstance(param, Operation):
            return param.sql
        if isinstance(param, Column):
            return param.full_column_name
        if isinstance(param, list) or isinstance(param, set) or isinstance(param, dict):
            return '(%s)' % ', '.join(['?'] * len(param))
        return '?'

    @staticmethod
    def _param_to_args(param):
        from .column import Column
        if isinstance(param, Operation):
            return param.args
        if isinstance(param, Column):
            return []
        if isinstance(param, list) or isinstance(param, set) or isinstance(param, dict):
            return list(param)
        return [param]


class LessThan(BinaryOperation):
    __operator__ = '<'


class GreaterThan(BinaryOperation):
    __operator__ = '>'


class EqualTo(BinaryOperation):

    def __init__(self, column, value):
        super(EqualTo, self).__init__(column, value)
        self.__operator__ = 'IS' if value is None else '='


class NotEqualTo(BinaryOperation):

    def __init__(self, column, value):
        super(NotEqualTo, self).__init__(column, value)
        self.__operator__ = 'IS NOT' if value is None else '!='


class LessThanOrEqualTo(BinaryOperation):
    __operator__ = '<='


class GreaterThanOrEqualTo(BinaryOperation):
    __operator__ = '>='


class In(BinaryOperation):
    __operator__ = 'IN'

    def __invert__(self):
        return NotIn(self.left, self.right)


class NotIn(BinaryOperation):
    __operator__ = 'NOT IN'


class Like(BinaryOperation):
    __operator__ = 'LIKE'

    def __invert__(self):
        return NotLike(self.left, self.right)


class NotLike(BinaryOperation):
    __operator__ = 'NOT LIKE'


class Or(BinaryOperation):
    __operator__ = ' OR '


class And(BinaryOperation):
    __operator__ = ' AND '


class UnaryOperation(Operation):

    def __init__(self, param):
        self._param = param

    @property
    def sql(self):
        operator = getattr(self, '__operator__')
        return operator.replace('?', self._param.sql)

    @property
    def args(self):
        return self._param.args


class Quote(UnaryOperation):
    __operator__ = '(?)'
