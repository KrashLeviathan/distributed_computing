# For files submitted to Marx, data containing test cases should go
# in a single list contained in a separate file. The list can contain
# whatever you want it to contain, but everything has to be within
# that one list. Marx will split that list when it distributes the
# code to the Worker Machines for execution.

data = [
    (1, 2, 'alpha', 3.4, True),
    (5, 6, 'beta', 7.8, False),
    (9, 10, 'gamma', 11.12, True),
    (13, 14, 'delta', 15.16, False)
]
