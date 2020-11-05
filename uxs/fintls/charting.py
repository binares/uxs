import ccxt
import math
import datetime
dt = datetime.datetime
td = datetime.timedelta

from fons.time import timestamp_ms

TIME_SLOPE_MULTIPLIER = 3600 * 1000


def calc_slope(p0, p1):
    """y_change / x_change (of two points)"""
    return (p1[1]- p0[1]) / (p1[0] - p0[0])


def calc_log_slope(p0, p1, base=10):
    """delta_logs_y / delta_x (of two points)"""
    # (log(p1[1], base) - log(p0[1], base)) / (p1[0] - p0[0])
    return math.log(p1[1] / p0[1], base) / (p1[0] - p0[0])


def _calc_b(a, p):
    # p.y = a * p.x + b
    return p.y - a * p.x


def _calc_log_b(a, p, base=10):
    # log(y, base) - log(b, base) = a * x
    # b = base ** (log(y, base) - a * x)    # <--use this
    # b = y / (base ** (a * x))             # simplified, but ** operation might cause OverFlowError
    return base ** (math.log(p.y, base) - a * p.x)


class BaseVector:
    __slots__ = ('x', 'y')
    
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    def __getitem__(self, key):
        return self.coords[key]
    
    def __iter__(self):
        return iter(self.coords)
    
    
    def __add__(self, other):
        if not isinstance(other, (BaseVector, BasePoint)):
            raise TypeError("unsupported operand type(s) for +: 'Vector' and '{}'".format(other.__class__.__name__))
        cls = self.__class__ if isinstance(other, BaseVector) else other.__class__
        return cls(self.x + other.x, self.y + other.y)
    
    
    def __sub__(self, other):
        if not isinstance(other, (BaseVector, BasePoint)):
            raise TypeError("unsupported operand type(s) for -: 'Vector' and '{}'".format(other.__class__.__name__))
        cls = self.__class__ if isinstance(other, BaseVector) else other.__class__
        return cls(self.x - other.x, self.y - other.y)
    
    
    def __mul__(self, other):
        if not isinstance(other, (int, float)):
            raise TypeError("unsupported operand type(s) for *: 'Vector' and '{}'".format(other.__class__.__name__))
        return self.__class__(self.x * other, self.y * other)
    
    
    def __truediv__(self, other):
        if not isinstance(other, (int, float)):
            raise TypeError("unsupported operand type(s) for /: 'Vector' and '{}'".format(other.__class__.__name__))
        return self.__class__(self.x / other, self.y / other)
    
    
    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.coords == other.coords
    
    
    def __str__(self):
        return 'V({}, {})'.format(self.x, self.y)
    
    @property
    def coords(self):
        return (self.x, self.y)


class Vector(BaseVector):
    def __init__(self, x, y):
        """
        :param x: timedelta (in milliseconds)
        :param y: pricedelta
        """
        if isinstance(x, td):
            x = int(x.total_seconds() * 1000)
        super().__init__(int(x), y)

    @property
    def t(self):
        return self.x
    @property
    def timedelta(self):
        return self.x


class BasePoint:
    __slots__ = ('x', 'y')
    vector_cls = BaseVector
    
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    
    def calc_distance(self, line, as_ratio=False):
        """Distance from self.y to line's y at self.x"""
        return calc_distance(self, line, as_ratio)
    
    
    def __getitem__(self, key):
        return self.coords[key]
    
    def __iter__(self):
        return iter(self.coords)
    
    
    def __add__(self, other):
        if not isinstance(other, BaseVector):
            raise TypeError("unsupported operand type(s) for +: 'Point' and '{}'".format(other.__class__.__name__))
        return self.__class__(self.x + other.x, self.y + other.y)
    
    
    def __sub__(self, other):
        if isinstance(other, BaseTrendline):
            return self.vector_cls(0, -calc_distance(self, other))
        if not isinstance(other, (BasePoint, BaseVector)):
            raise TypeError("unsupported operand type(s) for -: 'Point' and '{}'".format(other.__class__.__name__))
        cls = self.vector_cls if isinstance(other, BasePoint) else self.__class__
        return cls(self.x - other.x, self.y - other.y)
    
    
    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.coords == other.coords
    
    
    def __str__(self):
        return 'P({}, {})'.format(self.x, self.y)
    
    @property
    def coords(self):
        return (self.x, self.y)


class Point(BasePoint):
    __slots__ = ('x', 'y', 'datetime')
    vector_cls = Vector
    
    def __init__(self, x, y):
        """timetamp is in milliseconds"""
        if isinstance(x, dt):
            x = timestamp_ms(x)
        super().__init__(int(x), y)
        self.datetime = ccxt.Exchange.iso8601(self.x)
    
    @property
    def t(self):
        return self.x
    @property
    def timestamp(self):
        return self.x


class BaseTrendline:
    __slots__ = ('a', 'b', 'p', 'base', 'slope')
    units = (1, 1)
    point_cls = BasePoint
    _calc_b_on_init = True
    _is_log = False
    
    
    def __init__(self, *args, is_unit=True, base=None, p_x=None):
        """
        y = ax + b
        Args:
          slope, b 
          Point, slope ([::-1])
          Point, Point
        where slope = a * (units[0] / units[1])
        if is_unit=False then slope given is assumed to be `a`
        """
        if len(args) != 2:
            raise ValueError(args)
        if base is not None and not self._is_log:
            raise ValueError('`base` argument is only accepted in BaseLogTrendline subclasses')
        elif base is None and self._is_log:
            raise ValueError('`base` argument must not be None')
        _args = [x if not isinstance(x, tuple) else self.point_cls(*x) for x in args]
        is_point = [isinstance(x, BasePoint) for x in _args]
        a = None
        b = None
        point = None
        slope = None
        if sum(is_point) == 1:
            slope, point = _args if is_point[1] else _args[::-1]
            if not isinstance(slope, (int, float)):
                raise TypeError(args)
            if not is_unit:
                a, slope = slope, None
        elif sum(is_point) == 2:
            point, point_2 = _args
            if not self._is_log:
                a = calc_slope(point, point_2)
            else:
                a = calc_log_slope(point, point_2, base)
        elif all(isinstance(x, (int, float)) for x in _args):
            if is_unit:
                slope, b = _args
            else:
                a, b = _args
        else:
            raise TypeError(args)
        
        if a is None:
            a = slope / (self.units[0] / self.units[1])
        if slope is None:
            slope = a * (self.units[0] / self.units[1])
        if b is None and self._calc_b_on_init:
            b = self._calc_b(a, point, base)
        if point is None and b is not None:
            point = self.point_cls(0, b)
        
        self.a = a
        self.b = b
        self.p = point
        self.base = base
        self.slope = slope
        
        if p_x is not None and p_x != self.p.x:
            self.p = self.point_cls(p_x, self.calc_y(p_x))
    
    
    def _calc_b(self, a, point, base):
        if not self._is_log:
            return _calc_b(a, point)
        else:
            return _calc_log_b(a, point, base)
    
    
    def calc_y(self, x):
        return ((x - self.p.x) * self.a) + self.p.y
    
    
    def calc_coords(self, x):
        return self.point_cls(x, self.calc_y(x))
    
    
    def calc_distance(self, point, as_ratio=False):
        """Distance from line.y (at point.x) to point.y"""
        return calc_distance(self, point, as_ratio)
    
    
    def __add__(self, other):
        if isinstance(other, (int, float)):
            return self.__class__(self.slope, self.b + other, p_x=self.p.x)
        raise TypeError("unsupported operand type(s) for +: 'Trendline' and '{}'".format(other.__class__.__name__))
    
    
    def __sub__(self, other):
        if isinstance(other, (int, float)):
            return self.__class__(self.slope, self.b - other, p_x=self.p.x)
        elif isinstance(other, BasePoint):
            return self.point_cls.vector_cls(0, -calc_distance(self, other))
        raise TypeError("unsupported operand type(s) for -: 'Trendline' and '{}'".format(other.__class__.__name__))
    
    
    def __mul__(self, other):
        if isinstance(other, (int, float)):
            return self.__class__(self.slope * other, self.b * other, p_x=self.p.x)
        raise TypeError("unsupported operand type(s) for *: 'Trendline' and '{}'".format(other.__class__.__name__))
    
    
    def __truediv__(self, other):
        if isinstance(other, (int, float)):
            return self.__class__(self.slope / other, self.b / other, p_x=self.p.x)
        raise TypeError("unsupported operand type(s) for /: 'Trendline' and '{}'".format(other.__class__.__name__))
    
    
    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.ab == other.ab
    
    
    def __str__(self):
        return 'L({}, {})'.format(self.slope, self.p)
    
    @property
    def ab(self):
        return (self.a, self.b)
    @property
    def s(self):
        return self.slope


class BaseLogTrendline(BaseTrendline):
    _is_log = True
    _calc_b_on_init = False
    
    def __init__(self, *args, base=10, is_unit=True, p_x=None):
        """
        log(y, base) - log(b, base) = a * x
        y = (base ** (a * x)) * b
        However calculating `b` might be off by a large margin due decades of exponential growth/decay (back to 1970).
        Therefore a single reference point is used instead.
        log(y, base) - log(p.y, base) = a * (x - p.x)
        y = (base ** (a * (x - p.x))) * p.y
        Args:
          slope, b
          Point, slope ([::-1])
          Point, Point
        where slope = a * (units[0] / units[1])
        """
        super().__init__(*args, base=base, is_unit=is_unit, p_x=p_x)
    
    
    def calc_y(self, x):
        return (self.base ** (self.a * (x - self.p.x))) * self.p.y
    
    
    def __add__(self, other):
        raise NotImplementedError('__add__ operation of BaseLogTrendline is not implemented yet')
        #raise TypeError("unsupported operand type(s) for +: 'Trendline' and '{}'".format(other.__class__.__name__))
    
    
    def __sub__(self, other):
        if isinstance(other, BasePoint):
            return self.point_cls.vector_cls(0, -calc_distance(self, other))
        raise NotImplementedError('__sub__ operation of BaseLogTrendline is not implemented yet')
        #raise TypeError("unsupported operand type(s) for -: 'Trendline' and '{}'".format(other.__class__.__name__))
    
    
    def __mul__(self, other):
        raise NotImplementedError('__mul__ operation of BaseLogTrendline is not implemented yet')
        #raise TypeError("unsupported operand type(s) for *: 'Trendline' and '{}'".format(other.__class__.__name__))
    
    
    def __truediv__(self, other):
        raise NotImplementedError('__truediv__ operation of BaseLogTrendline is not implemented yet')
        #raise TypeError("unsupported operand type(s) for /: 'Trendline' and '{}'".format(other.__class__.__name__))
    
    
    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.a == other.a and other.calc_y(self.p.x) == self.p.y


class Trendline(BaseTrendline):
    """:param slope: delta_y / 1H"""
    units = (TIME_SLOPE_MULTIPLIER, 1)
    point_cls = Point


class LogTrendline(BaseLogTrendline):
    """:param slope: delta_y / 1H"""
    units = (TIME_SLOPE_MULTIPLIER, 1)
    point_cls = Point


def calc_distance(p, q, as_ratio=False):
    """distance from p to q. One must be Point, the other Line"""
    is_p_line = isinstance(p, BaseTrendline)
    is_q_line = isinstance(q, BaseTrendline)
    if sum([is_p_line, is_q_line]) != 1:
        raise TypeError('p OR q must be trendline. got types: p: {}, q: {}'.format(type(p), type(q)))
    y_p = p[1] if not is_p_line else p.calc_y(q[0])
    y_q = q[1] if not is_q_line else q.calc_y(p[0])
    if as_ratio:
        return (y_q - y_p) / y_p
    return y_q - y_p


def calc_distance_ratio(p, q):
    return calc_distance(p, q, True)


def is_near(point, line, max_distance, is_ratio=True, include=False):
    """Test if point is near line. max_distance as percentage from the *line*, e.g. 0.02 (2%)"""
    distance = calc_distance(line, point, is_ratio)
    if include:
        return abs(distance) <= abs(max_distance)
    return abs(distance) < abs(max_distance)


def is_above(point, line, min_distance=0, is_ratio=True, include=False):
    """Test if point is above line. min_distance as percentage from the *line*, e.g. 0.02 (2%)"""
    if min_distance < 0:
        raise ValueError('min_distance must be non-negative')
    distance = calc_distance(line, point, is_ratio)
    if include:
        return distance >= min_distance
    return distance > min_distance


def is_below(point, line, min_distance=0, is_ratio=True, include=False):
    """Test if point is below line. min_distance as percentage from the *line*, e.g. 0.02 (2%)"""
    if min_distance < 0:
        raise ValueError('min_distance must be non-negative')
    distance = calc_distance(line, point, is_ratio)
    if include:
        return distance <= -1* min_distance
    return distance < -1* min_distance


def is_above_at(point, line, min_distance=0, is_ratio=True):
    """Test if point is above or at line. min_distance as percentage from the *line*, e.g. 0.02 (2%)"""
    return is_above(point, line, min_distance, is_ratio, True)


def is_below_at(point, line, min_distance=0, is_ratio=True):
    """Test if point is below or at line. min_distance as percentage from the *line*, e.g. 0.02 (2%)"""
    return is_below(point, line, min_distance, is_ratio, True)


"""class Channel:
    def __init__(self, line1, line2):
        ":type: line1: Trendline"
        self.line1 = line1
        self.line2 = line2"""
