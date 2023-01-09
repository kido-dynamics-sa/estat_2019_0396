import pandas as pd

from estat_2019_0396 import mercator


def test_haversine_nyla():
    ny = [-74.017161, 40.704705]
    la = [-118.196496, 33.768214]
    distance_nyla = mercator.haversine([ny], [la])
    assert 3_920_000 < distance_nyla < 3_960_000


def test_distance_codes_self():
    x = 8
    y = 6
    z = 4
    code1 = pd.Series([x * 2**z + y])
    assert mercator.distance_codes(code1, code1, z=z) == 0


def test_distance_codes_neighbors():
    x = 8
    y = 6
    z = 4
    tolerance = 1.0e-8
    code1 = pd.Series([x * 2**z + y] * 8)
    neighbors = pd.DataFrame(
        [
            (x - 1, y - 1),
            (x - 1, y),
            (x - 1, y + 1),
            (x, y - 1),
            (x, y + 1),
            (x + 1, y - 1),
            (x + 1, y),
            (x + 1, y + 1),
        ],
        columns=["x", "y"],
    )
    neighbors["code"] = neighbors["x"] * 2**z + neighbors["y"]
    distance = mercator.distance_codes(code1, neighbors["code"], z=z)
    print(distance)
    assert (distance < tolerance).all()


def test_distance_codes_nonzero():
    x = 8
    y = 6
    z = 4
    tolerance = 0.01  # = 1cm
    code1 = x * 2**z + y
    neighbors = pd.DataFrame(
        [
            (x - 2, y - 2),
            (x - 2, y),
            (x - 2, y + 2),
            (x, y - 2),
            (x, y + 2),
            (x + 2, y - 2),
            (x + 2, y),
            (x + 2, y + 2),
        ],
        columns=["x", "y"],
    )
    neighbors["code"] = neighbors["x"] * 2**z + neighbors["y"]
    print(code1)
    print(neighbors)
    distance = mercator.distance_codes(code1, neighbors["code"], z=z)
    print(distance)
    assert (distance > tolerance).all()


def test_distance_codes_nyla():
    ny = [-74.017161, 40.704705]
    la = [-118.196496, 33.768214]
    longs = pd.Series([ny[0], la[0]])
    lats = pd.Series([ny[1], la[1]])
    codes = mercator.encode(longs, lats, z=32)
    print(codes)
    distance_nyla = mercator.distance_codes(
        pd.Series(codes[0]), pd.Series(codes[1]), z=32
    )[0]
    print(distance_nyla)
    assert 3_920_000 < distance_nyla < 3_960_000


def test_distance_codes_zs():
    ny = [-74.017161, 40.704705]
    la = [-118.196496, 33.768214]
    longs = pd.Series([ny[0], la[0]])
    lats = pd.Series([ny[1], la[1]])
    prev_distance = mercator.haversine([ny], [la])[0]
    for z in [30, 20, 15, 12, 10, 9, 8, 7, 6, 5]:
        codes = mercator.encode(longs, lats, z=z)
        distance = mercator.distance_codes(
            pd.Series(codes[0]), pd.Series(codes[1]), z=z
        )[0]
        print(z, prev_distance, distance)
        assert distance < prev_distance
        prev_distance = distance
