class Util:
    @staticmethod
    def add(var_1: int, var_2: int) -> int:
        return var_1 + var_2

    @staticmethod
    def func(x: int):
        if x == 5:
            raise ValueError("X must be a value other than 5")
        return x

    @staticmethod
    def my_division_function(a, b):
        return a / b
