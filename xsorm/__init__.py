#!/usr/bin/env python
# -*- coding: utf-8 -*-


from .model import (
    declarative_base,
)

from .fields import (
    IntField,
    CharField,
    VarcharField,
    ForeignKey,
    CASCADE,
    NO_ACTION,
    SET_NULL,
    SET_DEFAULT,
)

from .session import (
    SessionMaker
)

import logging

_console = logging.StreamHandler()
_logger = logging.getLogger('xsorm')
_logger.addHandler(_console)
_logger.setLevel(logging.DEBUG)
