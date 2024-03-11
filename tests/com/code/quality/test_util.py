from src.com.code.quality.util import Util
import pytest


def test_add():
    assert 17 == Util.add(8, 9)


def test_func():
    with pytest.raises(ValueError, match="X must be a value other than 5"):
        Util.func(5)


def test_func_positive():
    assert 6 == Util.func(6)
