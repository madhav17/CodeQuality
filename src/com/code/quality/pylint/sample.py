"""This script prompts a user to enter a message to encode or decode
using a classic Caesar shift substitution (3 letter shift)"""

import string


# pylint: disable=too-few-public-methods
class Sample:
    """Sample class"""

    def __init__(self):
        print("Initialize here")

    def test(self):
        """" sample test method"""
        shift = 3
        choice = input("would you like to encode or decode? : ")
        word = input("Please enter text : ")
        letters = string.ascii_letters + string.punctuation + string.digits
        encoded = ""
        if choice == "encode":
            for letter in word:
                if letter == " ":
                    encoded = encoded + " "
                else:
                    # name should be proper for it
                    index_of: int = letters.index(letter) + shift
                    encoded = encoded + letters[index_of]
        if choice == "decode":
            for letter in word:
                if letter == " ":
                    encoded = encoded + " "
                else:
                    index_of: int = letters.index(letter) - shift
                    encoded = encoded + letters[index_of]

        print(encoded)


if __name__ == '__main__':
    sample = Sample()
    sample.test()
