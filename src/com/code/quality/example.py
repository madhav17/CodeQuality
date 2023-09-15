class Example:
    def test(text: str, align: bool = True) -> str:
        if align:
            return f"{text.title()}\n{'-' * len(text)}"
        else:
            return f" {text.title()} ".center(50, "o")

    def f1(error: str) -> int:
        if len(error) > 4:
            return 3
        if len(error) > 10:
            return 1 + error
        return 99


if __name__ == "__main__":
    ob = Example
    print(ob.test("python type checking"))
    print(ob.test("use mypy", align="center"))
