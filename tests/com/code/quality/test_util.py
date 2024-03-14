from src.com.code.quality.Util import Util
import pytest


@pytest.mark.parametrize(
    "a, b, expected",
    [
        (8, 9, 17),
        (-80, -100, -180),
    ],
)
def test_add(a, b, expected):
    assert expected == Util.add(a, b)


def test_func():
    with pytest.raises(ValueError, match="X must be a value other than 5"):
        Util.func(5)


def test_func_positive():
    assert 6 == Util.func(6)


def test_code_raises_no_exception():
    try:
        assert 2 == Util.my_division_function(10, 5)
    except Exception as exc:
        assert isinstance(exc, ZeroDivisionError)


def test_code_raises_exception():
    try:
        Util.my_division_function(10, 0)
    except Exception as exc:
        assert isinstance(exc, ZeroDivisionError)
