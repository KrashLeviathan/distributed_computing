#!/usr/bin/env python


import hashlib


def do_something_2(c, d):
    return int(hashlib.md5(c.encode()).hexdigest(), 16) / (d * 1e+20)
