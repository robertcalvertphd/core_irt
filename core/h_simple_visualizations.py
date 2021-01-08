import numpy as np
import random as r
import core.h_file_handling as hfh

def create_density_list(n_list, interval = None, columns = None):
    assert interval is not None or columns is not None, "FATAL ERROR: create_density_list requires interval, or columns"
    if not type (n_list) == hfh.pd.Series:
        for number in n_list:
            t = type(number)
            valid = False
            if t == int or t == float:
                valid = True
            assert valid, "FATAL ERROR: list was fed a non integer value in create_density_list"

    np_array = np.array(n_list).astype(float)
    h_list = []

    _min = np_array.min()
    _max = np_array.max()
    if interval is None:
        interval = (_max-_min)/columns
    if columns is None:
        columns = int((_max-_min)/interval)

    sum = 0
    for c in range(columns+1):
        beginning = c*interval+_min
        end = (c+1)*interval+_min
        count = ((beginning <= np_array) & ( np_array < end)).sum()
        h_list.append(count)
        sum += count

    return np.array(h_list), interval, _min


def create_histogram(list_n, columns = None, interval = None, unit = None):
    density_np_ret = create_density_list(list_n, columns = columns, interval=interval)
    density_np_list = density_np_ret[0]
    interval = density_np_ret[1]
    assert interval > 0, "FATAL_ERROR: interval is not > 0 in create_histogram"
    _min = density_np_ret[2]
    if unit is None:
        unit = np.quantile(density_np_list,.05)
        if unit == 0:
            unit = np.mean(density_np_list)/10
    scaled_list = density_np_list/unit
    lines = []
    desc = "items"
    if unit == 1:
        desc = "item"
    print("x = " ,unit, desc,'.')
    for value in scaled_list:
        line = ""
        for x in range(int(value)):
            line += 'x'
        lines.append(line)
    counter = -1
    max_value = hfh.c_round(interval * len(lines) + _min,as_string=True)
    max_length = len(str(max_value))

    for line in lines:
        counter += 1
        current_id = interval * counter + _min
        current_id = hfh.c_round(current_id, as_string=True)
        spaces_needed = max_length - len(str(current_id))
        out = current_id
        for space in range(spaces_needed):
            out += ' '
        out += '\t' + line
        print(out)


def _test():
    list_n = []
    for i in range(1000):
        list_n.append(r.randint(1,6)+r.randint(1,6)+r.randint(1,6)/r.randint(1,10))
    create_histogram(list_n,columns=15)

#_test()