# -*- coding: utf-8 -*-

import unittest
from types import GeneratorType
from datetime import datetime, timedelta
import time

from scipy.interpolate import interp1d

from tspreproc import core


class InterpolatorTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__init__(self):
        t = core.Interpolator([
            {'timestamp': '2018-12-31 18:30:00', 'value': 89},
            {'timestamp': '2018-12-31 18:31:00', 'value': 80},
            {'timestamp': '2018-12-31 18:32:00', 'value': 85}
        ])
        self.assertTrue(isinstance(t, core.BaseTimeSeries))
        self.assertEqual(len(t), 3)
        self.assertEqual(t[0][1], 89)
        t2 = core.Interpolator([
            {'timestamp': '20181231 1830', 'value': 89},
            {'timestamp': '20181231 1831', 'value': 80},
            {'timestamp': '20181231 1832', 'value': 85}
        ], ts_format='%Y%m%d %H%M')
        self.assertEquals([d[0] for d in t], [d[0] for d in t2])
        t2 = core.Interpolator([
            {'timestamp': datetime(2018, 12, 31, 18, 31), 'value': 89},
            {'timestamp': datetime(2018, 12, 31, 18, 32), 'value': 80},
            {'timestamp': datetime(2018, 12, 31, 18, 33), 'value': 85}
        ], ts_format=lambda x: time.mktime(
            (x + timedelta(minutes=-1)).timetuple()
        ))
        self.assertEquals([d[0] for d in t], [d[0] for d in t2])
        t2 = core.Interpolator([
            {'TimeStamp': '2018-12-31 18:30:00', 'value': 89},
            {'TimeStamp': '2018-12-31 18:31:00', 'value': 80},
            {'TimeStamp': '2018-12-31 18:32:00', 'value': 85}
        ], ts_attr='TimeStamp')
        self.assertEquals([d[0] for d in t], [d[0] for d in t2])
        t2 = core.Interpolator([
            {'timestamp': '2018-12-31 18:30:00', 'Value': 89},
            {'timestamp': '2018-12-31 18:31:00', 'Value': 80},
            {'timestamp': '2018-12-31 18:32:00', 'Value': 85}
        ], value_attr='Value')
        self.assertEquals([d[1] for d in t], [d[1] for d in t2])
        t2 = core.Interpolator([
            ('2018-12-31 18:30:00', 89), ('2018-12-31 18:31:00', 80),
            ('2018-12-31 18:32:00', 85)
        ], ts_attr=0, value_attr=1)
        self.assertEquals([d[1] for d in t], [d[1] for d in t2])
        self.assertEquals([d[0] for d in t], [d[0] for d in t2])
        t2 = core.Interpolator([
            {'item': ('2018-12-31 18:30:00', 89)},
            {'item': ('2018-12-31 18:31:00', 80)},
            {'item': ('2018-12-31 18:32:00', 85)}
        ], ts_attr=lambda x: x['item'][0], value_attr=lambda x: x['item'][1])
        self.assertEquals([d[1] for d in t], [d[1] for d in t2])
        self.assertEquals([d[0] for d in t], [d[0] for d in t2])
        t2 = core.Interpolator([
            {'timestamp': '2018-12-31 18:30:00', 'value': '89'},
            {'timestamp': '2018-12-31 18:31:00', 'value': '80'},
            {'timestamp': '2018-12-31 18:32:00', 'value': '85'}
        ])
        self.assertEquals([d[1] for d in t], [d[1] for d in t2])
        t2 = core.Interpolator([
            {'timestamp': '2018-12-31 18:30:00', 'value': '**89**'},
            {'timestamp': '2018-12-31 18:31:00', 'value': '*80***'},
            {'timestamp': '2018-12-31 18:32:00', 'value': '***85*'}
        ], value_format=lambda x: float(x.strip('*')))
        self.assertEquals([d[1] for d in t], [d[1] for d in t2])
        t = core.Interpolator([
            {'timestamp': '2018-12-31 18:30:00', 'value': 89},
            {'timestamp': '2018-12-31 18:31:00', 'value': 80},
            {'timestamp': '2018-12-31 18:32:00', 'value': 85}
        ], kind='nearest')
        ip = interp1d([0, 60, 120], [89, 80, 85], kind='nearest')
        self.assertEqual(ip._kind, t.ip._kind)
        t.kind = 'linear'
        self.assertNotEqual(ip._kind, t.ip._kind)
        with self.assertRaises(TypeError):
            t2 = core.Interpolator(1234)
        with self.assertRaises(ValueError):
            t2 = core.Interpolator([
                {'timestamp': '1', 'value': 89},
                {'timestamp': '2', 'value': 80},
                {'timestamp': '3', 'value': 85}
            ], ts_format='%Y%m%d')
        with self.assertRaises(TypeError):
            t2 = core.Interpolator([], ts_format=123)

    def test__call__(self):
        t = core.Interpolator([
            {'timestamp': '2018-12-31 18:30:00', 'value': 89},
            {'timestamp': '2018-12-31 18:31:00', 'value': 80},
            {'timestamp': '2018-12-31 18:32:00', 'value': 85}
        ])
        ip = interp1d([0, 60, 120], [89, 80, 85])
        self.assertEqual(t('2018-12-31 18:30:30'), ip(30))
        t = core.Interpolator([])
        self.assertEqual(t('2018-12-31 18:30:30'), 0.0)

    def test__tidy_ts_value(self):
        t = core.Interpolator([
            {'timestamp': '2018-12-31 18:30:00', 'value': 89},
            {'timestamp': '2018-12-31 18:31:00', 'value': 80},
            {'timestamp': '2018-12-31 18:32:00', 'value': 85}
        ])
        expect = time.mktime(datetime(2018, 12, 31, 18, 30, 00).timetuple())
        self.assertEqual(t._tidy_ts_value(
            datetime(2018, 12, 31, 18, 30, 00)
        ), expect)
        self.assertEqual(t._tidy_ts_value(
            '001230311818', ts_format='%S%m%M%d%y%H'
        ), expect)
        self.assertEqual(t._tidy_ts_value(
            '2018-12-31 18:30:00'
        ), expect)

    def test__tidy_step(self):
        t = core.Interpolator([
            {'timestamp': '2018-12-31 18:30:00', 'value': 89},
            {'timestamp': '2018-12-31 18:31:00', 'value': 80},
            {'timestamp': '2018-12-31 18:32:00', 'value': 85}
        ])
        self.assertEqual(t._tidy_step(2.2), 2.2)
        self.assertEqual(t._tidy_step(3), 3)
        self.assertEqual(t._tidy_step('3.0', float), 3.0)
        self.assertEqual(
            t._tidy_step(timedelta(minutes=2)),
            timedelta(minutes=2).total_seconds()
        )
        self.assertEqual(
            t._tidy_step('2 minutes'),
            timedelta(minutes=2).total_seconds()
        )

    def test_generate(self):
        t = core.Interpolator([
            {'timestamp': '2018-12-31 18:30:00', 'value': 89},
            {'timestamp': '2018-12-31 18:31:00', 'value': 80},
            {'timestamp': '2018-12-31 18:32:00', 'value': 85}
        ])
        self.assertTrue(isinstance(t.generate(0, 1, 0.2), GeneratorType))
        g = t.generate('2018-12-31 18:30:00', '2018-12-31 18:32:00', '1 sec')
        res = list(g)
        ip = interp1d([0, 60, 120], [89, 80, 85])
        expects = [(
            datetime(2018, 12, 31, 18, 30, 00) + timedelta(seconds=i), ip(i)
        ) for i in range(120)]
        self.assertEquals(res, expects)
        g = t.generate(
            '2018-12-31 18:30:00', '2018-12-31 18:32:00', '1 sec',
            value_only=True
        )
        res = list(g)
        ip = interp1d([0, 60, 120], [89, 80, 85])
        expects = [ip(i) for i in range(120)]
        self.assertEquals(res, expects)


class GeneratorTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_generate(self):
        t = core.Aggregator([
            {'timestamp': '2018-12-31 18:30:00', 'value': 89},
            {'timestamp': '2018-12-31 18:30:30', 'value': 82},
            {'timestamp': '2018-12-31 18:31:00', 'value': 80},
            {'timestamp': '2018-12-31 18:31:30', 'value': 82},
            {'timestamp': '2018-12-31 18:32:00', 'value': 85}
        ], aggregation_func=lambda it: sum(d[1] for d in it))
        self.assertTrue(isinstance(t.generate(0, 1, 0.2), GeneratorType))
        g = t.generate('2018-12-31 18:30:00', '2018-12-31 18:33:00', '1 min',
                       value_only=True)
        res = list(g)
        expects = [float(89) + float(82), float(80) + float(82), float(85)]
        self.assertEquals(res, expects)
        g = t.generate('2018-12-31 18:30:00', '2018-12-31 18:33:00', '1 min',
                       value_only=False)
        res = list(g)
        expects = [(
            datetime(2018, 12, 31, 18, 30, 00) + timedelta(minutes=i),
            [float(89) + float(82), float(80) + float(82), float(85)][i]
        ) for i in range(3)]
        self.assertEquals(res, expects)
        g = t.generate('2018-12-31 18:30:00', '2018-12-31 18:33:00', '1 min')
        res = list(g)
        self.assertEquals(res, expects)
