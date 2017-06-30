#!/usr/bin/env python
# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractproperty
from collections import Iterable
from xsorm.operation import (
    LessThan,
    GreaterThan,
    LessThanOrEqualTo,
    GreaterThanOrEqualTo,
    EqualTo,
    NotEqualTo,
    In,
    Like,
)
from xsorm.exception import CompareError


class Column(metaclass=ABCMeta):
    def __lt__(self, other):
        if other is None:
            raise CompareError('Can not judge whether a column is less than null')
        return LessThan(self, other)

    def __gt__(self, other):
        if other is None:
            raise CompareError('Can not judge whether a column is greater than null')
        return GreaterThan(self, other)

    def __le__(self, other):
        if other is None:
            raise CompareError('Can not judge whether a column is less than or equal to null')
        return LessThanOrEqualTo(self, other)

    def __ge__(self, other):
        if other is None:
            raise CompareError('Can not judge whether a column is greater than or equal to null')
        return GreaterThanOrEqualTo(self, other)

    def __eq__(self, other):
        return self.is_(other)

    def __ne__(self, other):
        return self.is_not(other)

    @abstractproperty
    def full_column_name(self):
        pass

    def is_(self, other):
        return EqualTo(self, other)

    def is_not(self, other):
        return NotEqualTo(self, other)

    def in_(self, value_set):
        if not isinstance(value_set, Iterable):
            raise CompareError('Can not apply not in operation to an object that cannot be not iterable')
        return In(self, value_set)

    def like(self, other):
        return Like(self, other)
