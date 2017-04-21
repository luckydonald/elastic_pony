from datetime import date
from datetime import datetime
# from datetime import time
# from datetime import timedelta
from pony.orm import Json


def create_testclass_data(Testclass):
    a = Testclass(
        text="I am text in a text field",
        unicode="And I am ünicode in an text field",
        int_default=+127,
        int_8=+127,
        int_16=+32767,
        int_24=+8388607,
        int_32=+2147483647,
        int_64=+9223372036854775807,
        bool=True,
        float=199.33,
        # double=200495.995,
        date=date.today(),
        datetime=datetime.today(),
        # time=time.max,
        # timedelta=timedelta.max,
        Json={"look, dad": ["some", "json"], "wow": "indeed", "number": 4458, "littlepip_is_best_pony": True}
    )

    b = Testclass(
        text="This is even more text",
        unicode="öäüß",
        int_default=-128,
        int_8=-128,
        int_16=-32768,
        int_24=-8388608,
        int_32=-2147483648,
        int_64=-9223372036854775808,
        bool=False,
        float=-199.33,
        # double=200495.995,
        date=date.today(),
        datetime=datetime.today(),
        # time=time.max,
        # timedelta=timedelta.max,
        Json={"look, dad": 123, "wow": True, "number": 42, "best_pony": ["Littlepip", "The Lightbringer", "Toaster repair pony"]}
    )
    return a, b
