import sys
import tests
import unittest
from services.clean import clean


def main():
    try:
        action = sys.argv[1]
        if action == "-t" or action == "--test":
            suite = unittest.TestLoader().loadTestsFromModule(tests)
            unittest.TextTestRunner(verbosity=2).run(suite)
        elif action == "-s" or action == "--start":
            clean()
    except IndexError:
        print("SyntaxError: This is NOT a valid syntax.")
        print("Please use the following:")
        print("-t | --test   Tests variables and other functions to perform extraction")
        print("-s | --start  Start extraction based on variables")
        print("main.py [-t | --test | -s | --start]")


if __name__ == '__main__':
    main()
