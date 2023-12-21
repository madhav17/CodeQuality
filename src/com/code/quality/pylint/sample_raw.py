import string


class SampleRaw:

    def __init__(self):
        print("Intialize here")

    def test(self):
        shift = 3
        choice = input("would you like to encode or decode?")
        word = input("Please enter text")
        letters = string.ascii_letters + string.punctuation + string.digits
        encoded = ""
        if choice == "encode":
            for letter in word:
                if letter == " ":
                    encoded = encoded + " "
                else:
                    x = letters.index(letter) + shift
                    encoded = encoded + letters[x]
        if choice == "decode":
            for letter in word:
                if letter == " ":
                    encoded = encoded + " "
                else:
                    x = letters.index(letter) - shift
                    encoded = encoded + letters[x]

        print(encoded)


if __name__ == '__main__':
    ob = SampleRaw
    ob.test()
