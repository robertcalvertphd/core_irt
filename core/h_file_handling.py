import os
import pandas as pd
import numpy as np
from core.Enums import E
import math
nan = np.nan


def combine_response_string(path_to_files):
    dfs = []
    files = get_all_files(path_to_files)


def write_lines_to_text(lines, file_path, mode='w'):  # candidate for general helper file
    try:
        double_forward = '//'
        if file_path.find(double_forward) > -1:
            print("double forward slash found and removed")
            file_path.replace(double_forward, '/')
        with open(file_path, mode) as file:
            for line in lines:
                if line is not None:
                    file.write(line)
        return 1
    except:
        print("failure to write lines")
        return 0

def get_df(path_to_csv, header=None, index_col=None, dtype=object):
    return pd.DataFrame(pd.read_csv(path_to_csv, header=header, index_col = index_col, dtype=dtype))


def get_all_files(path_to_data, target_string=False, extension=False, debug_output = False):
    # todo: verify that this works with many layers of folders
    folders = get_all_folders_in_folder(path_to_data)

    if folders is not False:

        data_files = []
        for folder in folders:
            if debug_output:
                print(folder)
            if os.path.isdir(folder):
                sub_folders = get_all_folders_in_folder(folder)
                for s in sub_folders:
                    folders.append(s)
        folders.append(path_to_data)
        for folder in folders:
            if os.path.isdir(folder):
                files = get_all_file_names_in_folder(folder, target_string=target_string, extension=extension)
                for file in files:
                    if os.path.isfile(file):
                        data_files.append(file)
        return data_files
    return False


def get_all_folders_in_folder(folder_path):
    if os.path.isdir(folder_path):
        list = os.listdir(folder_path)
        ret = []
        for item in list:
            if os.path.isdir(folder_path + '/' + item):
                ret.append(folder_path + '/' + item)
        return ret
    else:
        print("get all folders fed path that DNE.")
        return False


def get_all_file_names_in_folder(folder_path, extension=False, target_string=False, can_be_empty=True):
    ret = []
    if os.path.isdir(folder_path):
        list = os.listdir(folder_path)

        if extension:
            for item in list:
                e = get_extension(item)
                if e == extension:
                    ret.append(folder_path + "/" + item)
        elif target_string:
            for item in list:
                f = item.find(target_string)
                if f > -1:
                    ret.append(folder_path + "/" + item)
        else:
            for item in list:
                ret.append(folder_path + "/" + item)
        return ret
    else:
        print(folder_path + " is not a folder.")
        if can_be_empty:
            return ret
        return False


def get_single_file(path, target_string = None, as_df = False, verbose = True, index_col = None, header = None, strict = False):
    a = get_all_file_names_in_folder(path, target_string = target_string)
    if index_col is not None:
        assert as_df, "index col requires as_df = True"
    for file in a:
        if file.find('~')>-1:
            a.remove(file)
            print("ignored file with ~ : " + file)
    if len(a) == 1:
        if as_df:
            return get_df(a[0], index_col = index_col, header=header)
        return a[0]
    if verbose:
        if len(a) == 0:
            print("no files matching " + path + " and " + target_string + ' were found.')
        else:
            print("more than one file matches " + path + " and " + target_string + '.')
    if strict:
        assert False,"get_single_file found no file matching " + target_string
    return False


def get_extension(name_with_extension):
    i = name_with_extension.find('.')
    if i > -1:
        return name_with_extension[i + 1:]
    return False


def pair_files(filesA, filesB, check=False, get_unhandled_items=False, four_digit_years=False, A_B_forms=False,
               Karen_naming=False, pair_full = False):
    if get_unhandled_items and check:
        print("can not simultaneously get unhandled and check that match")
    if pair_full:
        ret = []
        for fileA in filesA:
            nameA = get_stem(fileA)[:-2]
            for fileB in filesB:
                nameB = get_stem(fileB)[:-2]
                if nameA == nameB:
                    ret.append([fileA, fileB])
        if len(ret)> 0:
            return ret


    ret = []
    unhandled = []
    for fa in filesA:
        pe_mon = find_month(fa)
        pe_year = find_year(fa, four_digit_years, Karen_naming=Karen_naming)
        handled = 0
        for fb in filesB:
            data_mon = find_month(fb)
            data_year = find_year(fb, four_digit_years, Karen_naming=Karen_naming)
            if pe_mon == data_mon and pe_year == data_year:
                if A_B_forms:
                    form_a = find_form(fa)
                    form_b = find_form(fb)
                    if form_a == form_b:
                        ret.append([fa, fb])
                else:
                    ret.append([fa, fb])
                handled = 1
        if handled == 0:
            unhandled.append(fa)
    if len(filesA) == len(filesB):
        if len(ret) == len(filesA):
            return ret
        else:
            if not check:
                print("unhandled items in process raw_data")
                if get_unhandled_items:
                    return ret, unhandled
                else:
                    return ret
            else:
                return False
    elif check:
        print("length of files are unequal. Should not happen.")
        return False
    else:
        return ret


def find_year(file_name, four_digit_year=False, Karen_naming=False):
    #   check if month present
    m = find_month(file_name)
    cap_name = file_name.upper()
    if m is not False:
        mi = cap_name.rfind(m)
        if mi > 2:
            #   check if month is preceded by two numbers
            possible_year = file_name[mi - 2:mi]
            if four_digit_year and mi > 4:
                possible_year = file_name[mi - 4:mi]
            if Karen_naming:
                possible_year = file_name[mi + 3:mi + 7]
            if possible_year.isdigit():
                return possible_year
        else:
            print("no month present in file" + file_name)
        #   returns the first two consecutive numbers
        number_index = []
        for i in range(len(file_name)):
            if file_name[i].isdigit():
                number_index.append(i)
        if int(number_index[0] + 1) == int(number_index[1]):
            print("bad number finding")
            return file_name[number_index[0]] + file_name[number_index[1]]

    return False


def find_month(file_name):
    months = ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER", "OCTOBER",
              "NOVEMBER", "DECEMBER"]

    ret = []
    file_name = get_stem(file_name).upper()
    for month in months:
        full = file_name.find(month)
        if full > -1:
            ret.append(month)
        mon = month[:3]
        three = file_name.find(mon)
        if len(ret) == 0 and three > -1:
            ret.append(mon)
    if len(ret) > 1:
        return False
    if len(ret) == 1:
        return ret[0]
    else:
        return False


def find_form(file_path):
    #   assumes harsh naming convention
    #   NAMNNMON_F_x.ext
    file_name = get_stem(file_path)
    form_i = file_name.find('_')
    form = file_name[form_i + 1]
    return form


def get_stem(name_with_extension):  # candidate for general helper file
    try:
        i = name_with_extension.index('.')
        s = name_with_extension.rfind('/')
        if s == -1: s = 0
        ret = name_with_extension[s + 1:i]
        return ret
    except:
        return name_with_extension


def get_lines(file_name, ignore_pound = False, remove_BOM = True, ignore_blank_lines = False):  # candidate for general helper file
    lines = []
    try:
        with open(file_name) as f:
            for line in f:
                handled = False
                if remove_BOM:
                    if line[0] == 'ï':
                        line = line[3:]
                if ignore_blank_lines:
                    if len(line.split()) > 0:
                        if ignore_pound:
                            if line[0] != '#':
                                lines.append(line)
                        else:
                            lines.append(line)
                else:
                    lines.append(line)
        return lines
    except:
        print("invalid file type call in get_lines")
        return False


def get_parent_folder(file_path):
    last_slash = file_path.rfind('/')
    return file_path[:last_slash]


def get_index_of_line_that_starts_with_word_based_on_delimiter(lines, word, delimiter=','):
    index = -1
    looking = True
    while looking:
        index += 1
        split_line = lines[index].split(delimiter)
        if split_line[0] == word:
            looking = False
    return index


def get_next_blank_line_after_index_from_lines(lines, index):
    count = 0
    for line in lines[index:]:
        if line == "\n":
            return count
        count += 1
    return False


def get_stats_df(stats_path, bank_df=None, remove_na=True, remove_version=False):
    ret = []
    lines = get_lines(stats_path)
    header_index = get_index_of_line_that_starts_with_word_based_on_delimiter(lines, 'Sequence')
    blank_index = get_next_blank_line_after_index_from_lines(lines, header_index)
    data = lines[header_index + 1:blank_index + header_index]
    for line in data:
        ret.append(line.split(','))
    df = pd.DataFrame(ret)

    header = lines[header_index].split(',')
    try:
        df.columns = header[:-1]
    except:
        df.columns = header
    #   note the last entry has \n
    df = df[df['Scored'] != 'Removed']
    if remove_na:
        ids = df['Item ID']
        df = df.apply(pd.to_numeric, errors='coerce')
        # df = df.dropna['b'](axis=1, how='any')
        df = df[df['b'].notna()]
        df['Item ID'] = ids
    if bank_df is not None:
        #df = pd.merge([df,bank_df],on='AccNum')
        df.index = df['Item ID']
        bank_df.index = bank_df['AccNum']
        df['Domain'] = bank_df['Domain']
    if remove_version:
        old_ids = df['Item ID']
        new_ids = []
        for id in old_ids:
            new_ids.append(id[:-2])
        new_ids_S = pd.Series(new_ids)
        df['Item ID'] = new_ids_S
    return df


def add_info_from_bank_to_csv(bank_path, csv_path, list_of_target_columns, replace_csv = False, remove_tag = False, convert_to_int_str = True, add_0 = False):
    df = get_df(csv_path,header = 0)
    df.index = df['AccNum']
    df = df.drop(columns=['AccNum'])
    bank_df = pd.read_excel(bank_path, dtype = object)

    for item in list_of_target_columns:
        series = bank_df[item]
        #series = series.replace(nan, 9999)
        if convert_to_int_str:
            #series = pd.to_numeric(series,downcast='integer')
            series = series.astype(int).astype(str)

        series.index = bank_df['AccNum']
        df[item] = series
        if add_0:
            df[item] = '0' + df[item].astype(str)

    if replace_csv:
        name = csv_path
    else:
        name = get_parent_folder(csv_path) + "/"
        if remove_tag:
            name += get_stem(csv_path)[:-2] + '_m.csv'
        else:
            name += get_stem(csv_path) + '_m.csv'
    df.to_csv(name)


def remove_files_without_extenstion(path):
    files = get_all_file_names_in_folder(path)
    trash = []
    for file in files:
        if file.find('.')== -1:
            trash.append(file)
    for file in trash:
        os.remove(file)

def create_dir(path, verbose = False):
    if not os.path.isdir(path):
        os.mkdir(path)
        return True
    if verbose:
        print("directory " + path + " already exists")
    return False


def move_file(current, new_full_path):
    if not os.path.isfile(current):
        print("attempted to move file that does not exist")
    if os.path.isfile(new_full_path):
        print("attempting to move a file to a position in which it already exists.")
    os.rename(current, new_full_path)

def get_name(full_path_to_file, debug = False):
    stem = get_stem(full_path_to_file)
    extension = get_extension(full_path_to_file)
    ret = stem + '.' + extension
    if debug: print(ret)
    return ret

def remove_files_with_extension(path, extension):
    files = get_all_file_names_in_folder(path)
    trash = []
    for file in files:
        if file.find(extension)> -1:
            trash.append(file)
    for file in trash:
        os.remove(file)

def c_round(n,d=2, as_string = False, as_percentage = False):
    assert n is not False, "n can not be False in c_round"
    if n == 0:
        return 0
    assert float(n), "attempting to round a value that is not a number" + str(n)
    if as_percentage:
        n*=100
    n = str(n)
    i = n.find('.')
    w = n[:i]
    f = n[i:]
    if len(f) > d:
        f = f[:d+1]
    ret = w + f
    if as_string:
        percentage = ""
        if len(f)<d+1:
            ret += '0'
        if as_percentage:
            percentage = '%'
        return ret+percentage
    else:
        return float(ret)

def add_lines_to_csv(path, lines):
    assert type(lines) == list, "add_lines_to_csv takes a list of lines as an argument"
    assert os.path.isfile(path), path + " is not a valid file"
    ret_lines = get_lines(path)
    for line in lines:
        ret_lines.append(line+'\n')
    write_lines_to_text(ret_lines,path)

#print(c_round(21.23,2, as_string=True, as_percentage=False))



