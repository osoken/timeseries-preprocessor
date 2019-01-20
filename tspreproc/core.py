# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import time

from sortedcontainers import SortedList
from dateutil.parser import parse as dtparse
from pytimeparse import parse as tparse
from scipy import interpolate


class UserSortedList(object):
    def __init__(self, iterable=None, key=None):
        self.data = SortedList(iterable=iterable, key=key)
        self.__changed = True

    def is_changed(self):
        return self.__changed

    def mark_as_updated(self):
        self.__changed = True

    def add(self, value):
        self.__changed = True
        return self.data.add(value)

    def update(self, iterable):
        self.__changed = True
        return self.data.update(iterable)

    def clear(self):
        self.__changed = True
        return self.data.clear()

    def discard(self, value):
        self.__changed = True
        return self.data.discard(value)

    def __len__(self):
        return self.data.__len__()

    def remove(self, value):
        self.__changed = True
        return self.data.remove(value)

    def pop(self, index=-1):
        self.__changed = True
        return self.data.pop(index)

    def __iadd__(self, other):
        self.__changed = True
        return self.data.__iadd__(other)

    def __imul__(self, num):
        self.__changed = True
        return self.data.__imul__(num)

    def bisect_left(self, value):
        return self.data.bisect_left(value)

    def bisect_right(self, value):
        return self.data.bisect_right(value)

    def count(self, value):
        return self.data.count(value)

    def index(self, value, start=None, stop=None):
        return self.data.index(value, start=start, stop=stop)

    def irange(self, minimum=None, maximum=None, inclusive=(True, True),
               reverse=False):
        return self.data.irange(minimum=minimum, maximum=maximum,
                                inclusive=inclusive, reverse=reverse)

    def islice(self, start=None, stop=None, reverse=False):
        return self.data.islice(start=start, stop=stop, reverse=reverse)

    def __iter__(self):
        return self.data.__iter__()

    def __reversed__(self):
        return self.data.__reversed__()

    def __contains__(self, value):
        return self.data.__contains__(value)

    def __getitem__(self, index):
        return self.data.__getitem__(index)

    def __delitem__(self, index):
        return self.data.__delitem__(index)

    def __add__(self, other):
        return self.data.__add__(other)

    def __mul__(self, num):
        return self.data.__mul__(num)

    def __eq__(self, other):
        return self.data.__eq__(other)

    def __ne__(self, other):
        return self.data.__ne__(other)

    def __lt__(self, other):
        return self.data.__lt__(other)

    def __le__(self, other):
        return self.data.__le__(other)

    def __gt__(self, other):
        return self.data.__gt__(other)

    def __ge__(self, other):
        return self.data.__ge__(other)

    def copy(self):
        return self.data.copy()

    def __repr__(self):
        return self.data.__repr__()

    def append(self, value):
        return self.data.append(value)

    def extend(self, values):
        return self.data.extend(values)

    def insert(self, index, value):
        return self.data.insert(index, value)

    def reverse(self):
        return self.data.reverse()

    def __setitem__(self, index, value):
        return self.data.__setitem__(index, value)


class BaseTimeSeries(UserSortedList):
    """Base time series class.

    Parameters
    ----------
    seq: list
        The list of timeseries values.
    ts_format: None, callable, str
        Timestamp formatter. ``ts_format`` can be ``None``, ``callable``, or
        ``str``. ``callable`` must be a single argument function which returns
        ``float`` value. ``str`` must be an acceptable format string for
        ``datetime.strptime`` function. If ``None`` is passed,
        ``lambda x: dateutil.parser.parse(x).timestamp()`` is used.
    ts_attr: None, callable, str, int
        The attribute name for timestamp. It can be single argument function or
        ``str`` or ``int``. ``str`` or ``int`` is used to extract timestamp
        from an item (say ``x``) via ``x[ts_attr]``.
    value_format: None, callable
        Value formatter. Single argument functions, which returns ``float``,
        are acceptable. ``float`` function is used by default.
    value_attr: None, callable, str, int
        The attribute name for value. It can be single argument function or
        ``str`` or ``int``. ``str`` or ``int`` is used to extract value from
        an item (say ``x``) via ``x[value_attr]``.
    """
    def __init__(self, seq, ts_format=None, ts_attr=None, value_format=None,
                 value_attr=None):
        self.ts_format = ts_format
        self.ts_attr = ts_attr
        self.value_format = value_format
        self.value_attr = value_attr
        super(BaseTimeSeries, self).__init__(
            (self._mktuple(d) for d in seq), key=lambda d: d[0]
        )

    def _try_update(self):
        if self.is_changed():
            self._update()
            self.mark_as_updated()

    @property
    def ts_format(self):
        """ts_format attribute. ``callable`` or ``str`` which is acceptable
        as datetime format.
        """
        return self.__ts_format

    @ts_format.setter
    def ts_format(self, ts_format):
        self.__ts_format = self._tidy_ts_format(ts_format)

    @property
    def ts_attr(self):
        return self.__ts_attr

    @ts_attr.setter
    def ts_attr(self, ts_attr):
        if ts_attr is None:
            self.__ts_attr = lambda x: x['timestamp']
        elif callable(ts_attr):
            self.__ts_attr = ts_attr
        else:
            self.__ts_attr = lambda x: x[ts_attr]

    @property
    def value_format(self):
        return self.__value_format

    @value_format.setter
    def value_format(self, value_format):
        if value_format is None:
            self.__value_format = float
        else:
            self.__value_format = value_format

    @property
    def value_attr(self):
        return self.__value_attr

    @value_attr.setter
    def value_attr(self, value_attr):
        if callable(value_attr):
            self.__value_attr = value_attr
        elif value_attr is None:
            self.__value_attr = lambda x: x['value']
        else:
            self.__value_attr = lambda x: x[value_attr]

    def _mktuple(self, d):
        return (
            self.ts_format(self.ts_attr(d)),
            self.value_format(self.value_attr(d))
        )

    def _tidy_ts_format(self, ts_format):
        if ts_format is None:
            return lambda x: time.mktime(dtparse(x).timetuple())
        elif callable(ts_format):
            return ts_format
        elif isinstance(ts_format, str):
            return lambda x: time.mktime(
                datetime.strptime(x, ts_format).timetuple()
            )
        else:
            raise TypeError(ts_format)

    def _tidy_ts_value(self, ts, ts_format=None):
        if isinstance(ts, (int, float)):
            return ts
        if isinstance(ts, datetime):
            return time.mktime(ts.timetuple())
        if ts_format is None:
            return self.ts_format(ts)
        tsf = self._tidy_ts_format(ts_format)
        return tsf(ts)

    def _tidy_step(self, step, step_format=None):
        if step_format is not None:
            return step_format(step)
        if isinstance(step, (int, float)):
            return step
        if isinstance(step, timedelta):
            return step.total_seconds()
        return tparse(step)


class Interpolator(BaseTimeSeries):
    """Time series interpolator class.

    Parameters
    ----------
    seq: list
        The list of timeseries values.
    ts_format: None, callable, str
        Timestamp formatter. ``ts_format`` can be ``None``, ``callable``, or
        ``str``. ``callable`` must be a single argument function which returns
        ``float`` value. ``str`` must be an acceptable format string for
        ``datetime.strptime`` function. If ``None`` is passed,
        ``lambda x: dateutil.parser.parse(x).timestamp()`` is used.
    ts_attr: None, callable, str, int
        The attribute name for timestamp. It can be single argument function or
        ``str`` or ``int``. ``str`` or ``int`` is used to extract timestamp
        from an item (say ``x``) via ``x[ts_attr]``.
    value_format: None, callable
        Value formatter. Single argument functions, which returns ``float``,
        are acceptable. ``float`` function is used by default.
    value_attr: None, callable, str, int
        The attribute name for value. It can be single argument function or
        ``str`` or ``int``. ``str`` or ``int`` is used to extract value from
        an item (say ``x``) via ``x[value_attr]``.
    """
    def __init__(self, seq, ts_format=None, ts_attr=None, value_format=None,
                 value_attr=None, kind=None):
        super(Interpolator, self).__init__(
            seq=seq, ts_format=ts_format, ts_attr=ts_attr,
            value_format=value_format, value_attr=value_attr
        )
        self.__kind = 'linear' if kind is None else kind
        self._update()

    @property
    def kind(self):
        """kind property which defines ``kind`` of the interpolator.
        """
        return self.__kind

    @kind.setter
    def kind(self, kind):
        if self.__kind != kind:
            self.__kind = kind
            self._try_update()

    @property
    def ip(self):
        """interpolator. read only.
        """
        return self.__ip

    def __call__(self, ts, ts_format=None):
        return self.ip(self._tidy_ts_value(ts, ts_format))

    def _update(self):
        if len(self.data) > 0:
            self.__ip = interpolate.interp1d(
                [d[0] for d in self.data], [d[1] for d in self.data],
                kind=self.kind, fill_value='extrapolate'
            )
        else:
            self.__ip = lambda x: 0.0

    def generate(self, start, end, step, ts_format=None, step_format=None,
                 value_only=False):
        """returns a generator of the sequence from ``start`` to ``end`` with
        interval ``step``.

        Parameters
        ----------
        start: datetime, int, float, str
            The start of the sequence. ``int`` or ``float`` value is treated
            as UNIX timestamp. Other values are converted by ``ts_format``
        """
        s = self._tidy_ts_value(start, ts_format=ts_format)
        e = self._tidy_ts_value(end, ts_format=ts_format)
        diff = self._tidy_step(step, step_format=step_format)
        i = s
        if not value_only:
            while i < e:
                yield (datetime.fromtimestamp(i), self(i))
                i += diff
        else:
            while i < e:
                yield self(i)
                i += diff


class Aggregator(BaseTimeSeries):
    """Time series aggregator class.

    Parameters
    ----------
    seq: list
        The list of timeseries values.
    ts_format: None, callable, str
        Timestamp formatter. ``ts_format`` can be ``None``, ``callable``, or
        ``str``. ``callable`` must be a single argument function which returns
        ``float`` value. ``str`` must be an acceptable format string for
        ``datetime.strptime`` function. If ``None`` is passed,
        ``lambda x: dateutil.parser.parse(x).timestamp()`` is used.
    ts_attr: None, callable, str, int
        The attribute name for timestamp. It can be single argument function or
        ``str`` or ``int``. ``str`` or ``int`` is used to extract timestamp
        from an item (say ``x``) via ``x[ts_attr]``.
    value_format: None, callable
        Value formatter. Single argument functions, which returns ``float``,
        are acceptable. ``float`` function is used by default.
    value_attr: None, callable, str, int
        The attribute name for value. It can be single argument function or
        ``str`` or ``int``. ``str`` or ``int`` is used to extract value from
        an item (say ``x``) via ``x[value_attr]``.
    """
    def __init__(self, seq, ts_format=None, ts_attr=None, value_format=None,
                 value_attr=None, aggregation_func=None):
        super(Aggregator, self).__init__(
            seq=seq, ts_format=ts_format, ts_attr=ts_attr,
            value_format=value_format, value_attr=value_attr
        )
        self.aggregation_func = aggregation_func
        self._update()

    def __call__(self, start, stop, ts_format=None):
        start = self._tidy_ts_value(start, ts_format)
        stop = self._tidy_ts_value(stop, ts_format)
        return self.aggregation_func(
            self.irange((start, None), (stop, None), inclusive=(True, False))
        )

    def _update(self):
        pass

    def generate(self, start, end, duration, step, ts_format=None,
                 step_format=None, value_only=False):
        """returns a generator of the sequence from ``start`` to ``end`` with
        interval ``step``.

        Parameters
        ----------
        start: datetime, int, float, str
            The start of the sequence. ``int`` or ``float`` value is treated
            as UNIX timestamp. Other values are converted by ``ts_format``
        """
        s = self._tidy_ts_value(start, ts_format=ts_format)
        e = self._tidy_ts_value(end, ts_format=ts_format)
        diff = self._tidy_step(step, step_format=step_format)
        dur = self._tidy_step(duration, step_format=step_format)
        i = s
        if not value_only:
            while i < e:
                yield (datetime.fromtimestamp(i), self(i, i+dur))
                i += diff
        else:
            while i < e:
                yield self(i, i+dur)
                i += diff
