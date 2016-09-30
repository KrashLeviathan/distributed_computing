#!/usr/bin/env python

from helper1 import do_something_1
from helper2 import do_something_2
from helper3 import do_something_3


def calculate(data_set):
    results = []
    for test_case in data_set:
        ab = do_something_1(test_case[0], test_case[1])
        cd = do_something_2(test_case[2], test_case[3])
        test_case_result = do_something_3(ab, cd, test_case[4])
        results.append(test_case_result)
    return results


def main():
    """
    Running a couple test cases from the data_file.py
    :return:
    """
    data = [
        (1, 2, 'alpha', 3.4, True),
        (5, 6, 'beta', 7.8, False)
    ]
    print(calculate(data))


if __name__ == "__main__":
    main()
