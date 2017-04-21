# -*- coding: utf-8 -*-
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from pony.orm import *


__author__ = 'luckydonald'


def create_testclass(db):
    class Testclass(db.Entity):
        id = PrimaryKey(int, auto=True)
        text = Required(str)
        unicode = Required(unicode)
        int_default = Required(int)
        int_8 = Required(int, size=8)
        int_16 = Required(int, size=16)
        int_24 = Required(int, size=24)
        int_32 = Required(int, size=32)
        int_64 = Required(int, size=64)
        bool = Required(bool)
        float = Required(float)
        date = Required(date)
        datetime = Required(datetime)
        # time = Required(time)  // Json
        # timedelta = Required(timedelta)  // Not sure how this should be stored anyway
        Json = Required(Json)
    # end class
    return Testclass
# end def
