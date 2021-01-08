def get_int(prompt, max, min = 1):
    while 1:
        try:
            ret = int(input(prompt))
            if max >= ret >= min:
                return ret
            else:
                print("response not in valid range. Min = ", min, ". Max = ", max, ".")
        except ValueError:
            print("invalid response. Only integers are acceptable.")

def get_float(prompt, max=None, min=None):
    while 1:
        try:
            ret = float(input(prompt))
            valid = True
            if max is not None:
                if ret > max:
                    valid = False
            if min is not None:
                if ret < min:
                    valid = False
            if valid:
                return ret
            else:
                print("response not in valid range. Min = ", min, ". Max = ", max, ".")
        except ValueError:
            print("invalid response. Only numbers are acceptable.")


def get_choice_from_list(_list):
    print("chose from the following:")
    counter = 0
    for item in _list:
        counter += 1
        print(str(counter),"): " + item)
    choice = get_int("Input...", max = len(_list))
    return choice, _list[choice-1]

def get_continue():
    choice = get_choice_from_list(["CONTINUE","STOP"])
    if choice[0] == 1:
        return True
    return False

