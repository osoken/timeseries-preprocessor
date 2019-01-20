# -*- coding: utf-8 -*-

__description__ = u'tspreproc'

__long_description__ = u'''tspreproc
'''

__author__ = u'osoken'
__email__ = u'osoken.devel@outlook.jp'
__version__ = '0.0.0'

__package_name__ = u'tspreproc'

try:
    import core

    Interpolator = core.Interpolator
    Aggregator = core.Aggregator
except Exception as e:
    x = e

    def _raise(*args, **kwargs):
        raise x

    Interpolator = _raise
    Aggregator = _raise
