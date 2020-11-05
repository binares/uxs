from uxs.fintls.charting import Point, Vector, Trendline, LogTrendline, is_near, is_above, is_below
from fons.time import timestamp_ms
import datetime
dt = datetime.datetime

P0 = Point(dt(2000, 1, 1), 1000)
P1 = Point(dt(2000, 1, 2), 1240)
V = P1 - P0
L = Trendline(P0, P1)
P2 = Point(P1.x, P1.y+20)


def test_vector_ops():
    assert V + V == V * 2
    assert (V * 2) / 2 == V


def test_points_add_subtraction():
    assert V == Vector(86400*1000, 240)
    assert P0 + V == P1


def test_trendline():
    L2 = Trendline(P0, 10)
    #print(L, L2)
    assert L == L2 # delta_y of 10 / 1H


def test_trendline_ops():
    assert L + 2 == Trendline(L.slope, L.b + 2)
    assert L - 2 == Trendline(L.slope, L.b - 2)
    assert L * 2 == Trendline(L.slope*2, L.b*2)
    assert L / 2 == Trendline(L.slope/2, L.b/2)


def test_trendline_calc_y():
    assert L.calc_y(timestamp_ms(dt(2000, 1, 1, 12))) == 1120


def test_calc_distance():
    assert L.calc_distance(P1) == 0
    assert L.calc_distance(P2) == 20
    assert L.calc_distance(P2, True) == (20 / P1.y)
    assert P2 - L == Vector(0, 20)
    assert L - P2 == Vector(0, -20)



lP0 = Point(dt(2000, 1, 1), 10)
lP1 = Point(dt(2000, 1, 1, 1), 100)
lL = LogTrendline(lP0, lP1)
lP2 = Point(lP1.x, lP1.y + 20)


def test_log_trendline():
    assert round(lL.slope, 6) == 1 # 1 exponent per hour


def test_calc_log_y():
    assert round(lL.calc_y(timestamp_ms(dt(2000, 1, 1, 2))), 6) == 1000


def test_calc_log_distance():
    assert round(lL.calc_distance(lP1), 6) == 0
    lP2 = lP1 + Vector(0, 20)
    assert round(lL.calc_distance(lP2), 6) == 20
    assert round((lP2 - lL).y, 6) == 20
    assert round((lL - lP2).y, 6) == -20


oP2 = P1 + Vector(0, -20)

def test_is_near():
    assert is_near(P2, L, 21, False) == True
    assert is_near(P2, L, 19, False) == False
    assert is_near(P2, L, 20, False, include=False) == False
    assert is_near(P2, L, 20, False, include=True) == True
    assert is_near(oP2, L, 21, False) == True
    assert is_near(oP2, L, 19, False) == False
    ratio = (20 / P1.y)
    assert is_near(P2, L, ratio+0.01) == True
    assert is_near(P2, L, ratio-0.01) == False


def test_is_above():
    assert is_above(P2, L, 21, False) == False
    assert is_above(P2, L, 19, False) == True
    assert is_above(P2, L, 20, False, include=False) == False
    assert is_above(P2, L, 20, False, include=True) == True
    assert is_above(oP2, L, 21, False) == False


def test_is_below():
    assert is_below(oP2, L, 21, False) == False
    assert is_below(oP2, L, 19, False) == True
    assert is_below(oP2, L, 20, False, include=False) == False
    assert is_below(oP2, L, 20, False, include=True) == True
    assert is_below(P2, L, 21, False) == False
