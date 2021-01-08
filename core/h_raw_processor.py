import core.h_file_handling as hfh
E = hfh.E
pd = hfh.pd

#todo: add consistent replacement for missing responses S has come up in some places... I am not sure why it is there.

#   This file processes raw response strings and bank exports into _f and _c files for each form and bank as a whole.
#   As additional data formats arise they should be addressed here.


def validate_raw_files(project_path):
    bank_files = hfh.get_all_files(project_path+'/bank_files')
    response_strings = hfh.get_all_files(project_path+'raw_data')
    pairs = hfh.pair_files(bank_files, response_strings)
    print("valid pairs of raw data = " + str(len(pairs)))


def is_valid_name(path, harsh=False):
    acceptable_extensions= ['txt','csv']
    ext = hfh.get_extension(path)
    if ext not in acceptable_extensions:
        return False
    name = hfh.get_stem(path)
    month = hfh.find_month(name)
    year = hfh.find_year(name)
    if month and year:
        i_year = name.find(year)
        tag = name[:i_year]
        full_name = tag+year+month
        if harsh:
            if full_name == name:
                return name
        else:
            return name
        # currently unreachable consider removing or incorporating
        if full_name == name[:-2]:
            print("AB form detected")
            return name
    return False


def process_response_string_file(f_path,
                                 bank_path = None,
                                 destination_path = None,
                                 write_csv = False,
                                 get_df = True,
                                 create_c = True,
                                 paired_bank_xlsx = None
                                 ):
    if create_c:
        assert destination_path is not None,"process response string needs to know where to put the processed data"
    name = hfh.get_stem(f_path)
    lines = hfh.get_lines(f_path)
    assert len(lines)>0,"asked to process empty file:" + f_path

    c_df = None
    f_df = None

    if is_type_K(lines):
        processed_lines = processK(lines)
        f_df = processed_lines
    elif is_type_A(lines):
        processed_lines = processA(lines)
        c_df = processed_lines[0]
        f_df = processed_lines[1]
    elif is_type_B(lines):
        processed_lines = processB(lines)
        f_df = processed_lines
    elif is_type_C(lines):
        processed_lines = processC(lines)
        c_df = processed_lines[0]
        f_df = processed_lines[1]
    elif is_type_D(lines):
        processed_lines = processD(lines)
        f_df = processed_lines
    elif is_type_E(lines):
        processed_lines = processE(lines)
        c_df = processed_lines[0]
        f_df = processed_lines[1]
    elif is_type_F(lines):
        processed_lines = processF(lines)
        f_df = processed_lines
    elif is_type_G(lines):
        processed_lines = processG(lines)
        c_df = processed_lines[0]
        f_df = processed_lines[1]
    elif is_type_H(lines):
        processed_lines = processH(lines)
        f_df = processed_lines
    elif is_type_I(lines):
        processed_lines = processI(lines)
        f_df = processed_lines
    elif is_type_J(lines):
        processed_lines = processJ(lines)
        f_df = processed_lines

    else:
        print(f_path + " is already formatted")
        is_formatteed(lines)
        f_df = hfh.get_df(f_path)

    if c_df is not None and bank_path:
        # add AccNum instead of sequence
        b_df = create_c_df_from_bank(bank_path)
        b_df['Key'] = c_df['Key']
        c_df = b_df
    if c_df is None and bank_path is not None and create_c:
        #todo: consider respecting the correct answer at the time vs the bank or just destroy it
        bank_files = hfh.get_all_files(bank_path, extension='xlsx')
        pair = hfh.pair_files([f_path], bank_files)
        if len(pair) == 0:
            print("could not find matching bank file and no default control information present.")
        if len(pair) == 1:
            # todo: may evaluate differences between bank and response string if desired
            c_df = create_c_df_from_bank(pair[0][1])
        if len(pair) > 1:
            print("more than one file matched for bank", f_path)

    #confirm_id_as_index
    if 0 in f_df.columns or '0' in f_df.columns:
        f_df = f_df.set_index(f_df[0], drop=True)
        f_df = f_df.drop(columns = 0)
    if write_csv:
        #todo changed index... need to make sure all processed items spit out the same... in this case they are pre-strict.

        f_df.to_csv(destination_path + '/'+name + '_f.csv', index=True, header=False)
        if c_df is not None:
            c_df.to_csv(destination_path + '/' + name + '_c.csv', index=None, header=False)
    if get_df:
        return f_df


def process_response_strings_for_IRT(path_to_raw_data, processed = None, bank = None, verbose = False, get_f_df = False):
    #todo edited while tired confirm it works later

    path = path_to_raw_data
    if path is not False:
        lines = hfh.get_lines(path)

        r = path.find('raw_data')
        #assumes that raw_data exists in IRT model
        name = path
        if r > -1:
            project_directory = path[:r]
            name = project_directory + "/processed_data/" + hfh.get_stem(path)
            processed = project_directory+'/processed_data/'
            bank = project_directory + '/bank_files/'
            valid = is_valid_name(path)
            while valid is False:
                print(path + " is a raw data name which does not conform to convention of CCCYYMON.")
                name = input("enter an appropriate name here")
                valid = is_valid_name(name)

        if lines is False:
            print("Error in determine response string.\n Path request error in path " + path)
        else:
            process_response_string_file(path, bank, write_csv=True, destination_path=processed)


def is_type_A(lines, verbose = True):
    first_line = lines[0]
    first_line = first_line.strip('_')
    if first_line[0].isdigit():
        return False
    if len(first_line.split())<3:
        return False
    if verbose:
        print(first_line)
        print("IS TYPE OF TYPE:     # A = CAB   CABSEP17   XXXX...")
    return True


def is_type_B(lines, verbose = True):
    first_line = lines[0]
    split = first_line.split()
    if len(split)>1:
        a = split[0]
        b = split[1]
        if isinstance(b,str) and len(b)>1:
            response = b[0]
        else:
            return False
    else:
        return False
    if not first_line[0].isdigit():
        return False
    if len(split)<2:
        return False
    if response.isdigit():
        return False
    if verbose:
        print(first_line)
        print("IS TYPE OF TYPE:    # B = 27782   XXXX....")
    return True


def is_type_C(lines, verbose = True):
    if len(lines)<5:
        return False
    first_line = lines[0]
    fourth_line = lines[3]
    fifth_line = lines[4]
    split = first_line.split()
    if len(split)<4:
        return False
    if not split[0].isdigit():
        return False
    if split[1].isdigit() or split[2].isdigit():
        return False
    if fourth_line[0].isdigit():
        return False
    if fourth_line[0].upper() != 'Y' and fourth_line[0].upper() != 'N':
        return False
    len_5 = len(fifth_line)
    if len_5<3:
        return False
    if verbose:
        print(first_line)
        print("IS TYPE OF TYPE:        # C = 170 N O 11")

    return True


def is_type_D(lines, verbose = True):
    first_line = lines[0]
    split = first_line.split(',')
    if len(split)!=2:
        return False
    if verbose:
        print(first_line)
        print("IS TYPE OF TYPE:        # D = ID, response string")
    return True


def is_type_E(lines, verbose = True):

    first_line = lines[0]
    split = first_line.split()
    months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    if len(split) != 2:
        return False
    month_present = False
    for month in months:
        line = first_line.upper()
        if line.find(month)>-1:
            month_present = True
    if not month_present:
        return False
    if verbose:
        print(first_line)
        print("IS TYPE OF TYPE:        # E = CCCYYMON   XXX...")
    return True


def is_type_F(lines, verbose = True):
    first_line = lines[0]
    split = first_line.split(',')
    if len(split)<10:
        return False
    if split[2].isdigit():
        return False
    if split[2].upper() != 'Y' and split[2].upper() != 'N':
        return False
    if verbose:
        print(first_line)
        print("IS TYPE OF TYPE:        #F = 50056315,M,Y,X,X,X")
    return True


def is_type_G(lines, verbose = True):
    if len(lines)<5:
        return False
    fifth_line = lines[4]
    split = fifth_line.split()

    if len(split) <4:
        return False
    if not split[0].isdigit():
        return  False
    if not split[1].isdigit():
        return False
    if split[2].isdigit():
        return False
    if split[3].isdigit():
        return False
    return True

def is_type_H(lines, verbose = True):
    # old style id  RRRRRRRRRR
    first_line = lines[0]
    s = first_line.split()
    if len(s[0]) != 8:
        return False
    if len(s[1]) < 10 and len(lines)>1:
        s2 = lines[1].split()
        if len(s2[1]) <10:
            print("fuzzy logic says no. Examine" + lines[1] + " for type ID RRRR")
            return False
    if len(s)>2 and len(lines)>1:
        s2 = lines[1].split()
        if len(s2) > 2:
            print("fuzzy logic says no. Examine" + lines[1] + " for type ID RRRR")
            return False
    return True


def is_type_I(lines, verbose = True):
    #NAM,NAMNNMON, K,K,K,...
    first_line = lines[0]
    s = first_line.split(',')
    if len(s)<3:
        return False
    if s[0].isdigit():
        return False
    if s[1].isdigit():
        return False
    return True

def is_type_J(lines, verbose = True):
    #this is sloppy but I should have one for it already so...

    first_line = lines[0]
    comma_split = first_line.split(',')
    if len(comma_split)>1:
        return False
    split = first_line.split()
    id = split[0].strip('_')
    if len(split)<2:
        return False
    if not id.isdigit():
        return False
    if split[1].isdigit():
        return False
    return True

def is_type_K(lines, verbose = True):
    #Pearson Vue
    # of type
    #RegID, ClientID, Attempt, itemIDs…
    #KEY
    #ID, ####,#,R,R,R…
    first_line = lines[0]
    split0 = first_line.split(',')
    first_item = split0[0].strip()
    if first_item == 'RegID':
        return True
    return False

def is_formatteed(lines, verbose = True):
    first_line = lines[0]
    split = first_line.split(',')
    if len(split) < 10:
        return False
    if split[2].isdigit():
        return False
    if split[2].upper() == 'Y' or split[2].upper() == 'N':
        return False
    if verbose:
        print(first_line)
        print("already formatted:", first_line)
    return True


def processA(lines):
    # A = CAB   CABSEP17   XXXX...
    line1 = lines[0]
    split_line = line1.split('   ')
    correct_string = split_line[2].strip()
    if correct_string == 'R' or correct_string =='F':
        return False
    ts = []
    for line in lines[1:]:
        if not line == '\n':
            split_line = line.split('   ')
            id = split_line[0]
            response_string = split_line[1]
            ts.append([id,response_string])
    f_df = create_f_df(ts)

    c_df = create_c_df_from_correct(correct_string)
    return c_df, f_df


def processB(lines):
    # B = 27782   XXXX....
    ts = []
    estimated_number_of_items = 0
    maximum_id_length = get_max_id_length(lines)

    count = -1
    for line in lines:
        count += 1
        #   blanks make this problematic

        #line = set_id_length_on_line(line,maximum_id_length)
        line = line.strip()
        split = line.split()
        #   determine number of items
        omissions = []
        if len(split)>1:
            id = split[0]

            response = line[len(id):].strip()
            if len(response) > estimated_number_of_items:
                estimated_number_of_items = len(response)
            if len(id)>maximum_id_length:
                maximum_id_length = len(id)
            ts.append([id, response])


    # handle omissions
    for o in omissions:
        split = o.split()




    if len(lines)>0:
        f_df = create_f_df(ts)
        return f_df


def processC(lines):
    # C = 170 N O 11
    # XXX...
    # 444...
    # YYY...
    correct = lines[1]
    options = lines[2]
    include = lines[3]
    f_df = processB(lines[4:])
    i = -1
    lines = []
    for item in correct:
        i+=1
        lines.append([i+1,item,options[i],1,include[i],'M'])
    c_df = pd.DataFrame(lines)
    c_df.columns = ['Position', 'Key', 'Options', 'Domain', 'Include', 'Type']
    return c_df, f_df


def processD(lines):
    # D = ID, response string
    ts = []
    for line in lines:
        split_line = line.strip().split(',')
        ts.append([split_line[0],split_line[1]])
    f_df = create_f_df(ts)
    return f_df


def processE(lines):
    #E = CCYYMON   XXX...
    # was not correclty identifying missing responses
    correct = lines[0].split()[1]
    c_df = create_c_df_from_correct(correct)
    ts = []
    for line in lines[1:]:
        if not line == '\n':
            split_line = line.strip().split()
            if len(split_line)>1:
                id = split_line[0]
                response = line[len(id):].strip()
                ts.append([id, response])
            else:
                print("empty response string found for id", split_line[0])
    f_df = create_f_df(ts)
    return c_df, f_df


def processF(lines):
    # F = 50056315,M,Y,X,X,X
    ret = []
    items = 0
    for line in lines:
        ret_line = []
        split_line = line.split(',')
        id = split_line[0]
        ret_line.append(id)
        for r in split_line[3:]:
            items += 1
            ret_line.append(r.strip())
        ret.append(ret_line)

    f_df = pd.DataFrame(ret)
    return f_df


def processG(lines):
    #G: 0000  0000000123  NAME        LNAME      ID1234  RRRR")

    correct = lines[1].strip()
    c_df = create_c_df_from_correct(correct)
    ts = []
    for line in lines[4:]:
        if not line == '\n':
            split = line.strip().split()
            # people can have spaces in their names... there is absolutely no reason to have named data....
            last_digit = 0
            #   makes a hard assumption that all ids are digits or at least contain a digit
            #
            for i in range(len(split)):
                chunk = split[i]
                for c in chunk:
                    if c.isdigit():
                        last_digit = i
            id = split[last_digit]
            response_string = line[line.find(id)+len(id):].strip()
            ts.append([id, response_string])
    f_df = create_f_df(ts)
    return c_df, f_df

def processH(lines):
    #__ID   RRRRRRRRRRRRR
    ts= []
    for line in lines:
        if not line == '\n':
            split = line.strip().split()
            id = split[0]
            response_string = line[line.find(id)+len(id):].strip()
            ts.append([id, response_string])
    f_df = create_f_df(ts)
    return f_df

def processI(lines):
    #NAM, NAMNNMON, K,K,K,...
    #
    #id,r,r,r,...
    if len(lines[1]) > 2:
        lines = lines[1:]
    else:
        lines = lines[2:]
    ret = []
    items = 0
    for line in lines:
        ret_line = []
        print(line)
        split_line = line.split(',')
        id = split_line[0]
        ret_line.append(id)
        for r in split_line[1:]:
            items += 1
            ret_line.append(r.strip())
        ret.append(ret_line)

    f_df = pd.DataFrame(ret)
    # drop the last column if only contains F and R
    last_col = f_df[f_df.shape[1]-1]
    last_col = last_col.str.contains('F') | last_col.str.contains('R')
    if last_col.sum() == len(last_col):
        f_df = f_df.drop(columns=f_df.shape[1]-1)
    return f_df

def processJ(lines):
    #   of form __id   RRRR
    ts = []
    for line in lines:
        split = line.split()
        id = split[0].strip()
        response = line[len(id):].strip()
        ts.append([id,response])
    f_df = create_f_df(ts)
    return f_df

def processK(lines):
    #   of form Pearson Vue
    # info
    # Key
    # crap, id, crap, R,R...

    ret = []
    for line in lines[2:]:
        line = line.replace('"','').strip()

        #todo: this is probably where the issue is.
        line = line.strip(',')  # this should be most often not an issue. CHANGED 12-16
        split = line.split(',')
        id = split[1].strip()
        response = split[3:]
        response.insert(0,id)
        ret.append(response)
    f_df = hfh.pd.DataFrame(ret)
    return f_df

def create_f_df(l_t_id_response_string):
    #todo ensure end is not R or F
    ret = []
    for tuple in l_t_id_response_string:
        line = []
        line.append(tuple[0])
        response = tuple[1]
        R = response.find('R')
        F = response.find('F')
        i = max(R,F)
        if i > 0:
            response = response[:i].strip()
        for c in response:
            line.append(c)

        ret.append(line)
    # editing 12_18_20 todo: this may have broken things
    print("verify me or you will regret it!!!!\n create_f_df in hr")
    #cols = ['ID']
    df = pd.DataFrame(ret)
    #for i in range(df.shape[1]-1):
    #    cols.append(i + 1)
    #df.columns = cols
    return df


def create_c_df_from_correct(correct):
    ret = []
    counter = 0
    for c in correct:
        counter += 1
        ret.append([counter, c, 4, 1, 'Y','M'])
    df = pd.DataFrame(ret)
    df.columns = ['Position', 'Key', 'Options', 'Domain', 'Include', 'Type']
    return df


def create_c_df_from_bank(bank_path):
    b_df = pd.read_excel(bank_path)
    b_df['Options'] = 4
    b_df['Type'] = 'M'
    b_df['Key'] = b_df['CorrectAnswer']
    b_df['Include'] = 'Y'
    if 'UseCode' in b_df.columns:
        b_df.loc[b_df['UseCode'] == 'Pretest', 'Include'] = 'N'
    b_df = b_df[['AccNum','Key','Options','Domain','Include','Type']]
    return b_df


def update_c_from_bank(project_path):
    # assumes that updated _c files have position instead of accNum
    bank_directory = project_path+'/bank_files'
    processed_directory = project_path + '/processed_data'
    c_files = hfh.get_all_files(processed_directory, target_string= '_c.csv')
    b_files = hfh.get_all_files(bank_directory, target_string='.xlsx')
    pairs = hfh.pair_files(c_files, b_files)
    for pair in pairs:
        c_file = pair[0]
        b_file = pair[1]
        c_df = hfh.get_df(c_file)
        b_df = pd.read_excel(b_file)
        c_df.columns = ['AccNum','Key','Options','Domain','Include','Type']
        c_df['AccNum'] = b_df['AccNum']
        c_df = c_df[['AccNum', 'Key', 'Options', 'Domain', 'Include', 'Type']]

        c_df.to_csv(c_file, index=None, header=None)


def create_CAL(project_path, processed_path = None, destination_path = None, pair_full = True, debug = True):
    DATA_FILE = 0
    CONTROL_FILE = 1
    if processed_path == None:
        path = project_path+'/processed_data'
    data_files = hfh.get_all_file_names_in_folder(path, target_string='_f.csv')
    control_files = hfh.get_all_file_names_in_folder(path, target_string='_c.csv')
    df = pd.DataFrame([])
    control_dfs = []
    paired_files = hfh.pair_files(data_files, control_files, pair_full=pair_full)

    for pair in paired_files:
        print("CREATING CAL FROM PAIR: " + pair[0] + " " + pair[1])
        print(pair)
        control_path = pair[CONTROL_FILE]
        data_path = pair[DATA_FILE]
        f_df = hfh.get_df(data_path, header = None)
        c_df = hfh.get_df(control_path, header=None)

        control_dfs.append(c_df)
        f_df = get_strict_format_f_df(c_df, f_df)
        graded = strict_grade(f_df=f_df, c_df=c_df, operational=False, correct='1', incorrect='2')
        if graded is not False:
            a = graded.columns.duplicated()
            df = pd.concat([df, graded], axis=0)

    print("replacing")
    if len(paired_files)>0:
        c_df = pd.DataFrame([])
        c_df['AccNum'] = df.columns
        c_df['Key'] = '1'
        c_df['Options'] = '2'
        c_df['Domain'] = '1'
        c_df['Include'] = 'Y'
        c_df['Type'] = 'M'
        type = "_FINAL_"
        if destination_path.find('initial')>0:
            type = "_INITIAL_"
        name = destination_path +'/'+ hfh.get_stem(pair[0])[:3]+type

        print("replacing 2.0")
        df = df.replace(2.0, '2')
        print("replacing 1.0")
        df = df.replace(1.0, '1')
        print("filling NA")
        df = df.fillna(value='-')
        print("replacing empty with X")
        df = df.replace(" ", "X")
        print("writing csvs")
        df.to_csv( name + 'f.csv', index = True, header = False)
        c_df.to_csv(name + 'c.csv',index = None, header=False)

        return df, c_df
    else:
        print("failed to pair files in create_CAL")


def remove_header_from_files(files):
    for file in files:
        lines = hfh.get_lines(file)
        ret = lines[1:]
        ret = pd.DataFrame(ret)
        ret.to_csv(file)


def parse_file_to_csv():
    #this is a ultility in case I leave to allow for creation of csv from txt files for parsing
    pass


def grade_examination(f_df, c_df, correct = '1', incorrect = '2', only_operational = False, grading_processed = False):
    if not 'AccNum' in c_df.columns:
        c_df.columns = ['AccNum', 'Key', 'Options', 'Domain', 'Include', 'Type']
    ret = f_df
    if grading_processed is False:
        ret = ret.T
    # questionable ruling
    if 0 in ret.columns:
        ids = f_df.iloc[:, 0]
        ret = ret.drop(columns = 0)
    else:
        ids = ret.index.values
    #ret.columns = ids
    if only_operational:
        # at this stage rows are people and columns are items so we should switch it to drop and then switch back...

        include = c_df['Include']=='Y'
        ret = ret.T
        ret = ret.reset_index()
        if 'index' in ret.columns:
            ret = ret.drop(columns='index')
        ret = ret[include]
        c_df = c_df[include]
        ret = ret.T
        ret.columns = c_df['AccNum']
        #print("MEAN", test_average)

    current_position = -1

    #RULING: column names are always STRINGS. This goes against pandas somewhat.
    #ret = ret.T
    #ret.columns = ret.columns.astype(str)

    for answer in c_df['Key']:
        current_position += 1

        try:
            accNum = ret.columns[current_position]
            ret.loc[ret[accNum] == answer, ret.columns[current_position]] = correct
            ret.loc[ret[accNum] != correct, ret.columns[current_position]] = incorrect
        except:
            print("delete me")
    ret = ret.apply(hfh.pd.to_numeric, errors = 'coerce')
    ret = ret.dropna(axis = 1, how ='all')

    accNums = c_df['AccNum'].tolist()
    try:
        ret.columns = accNums
    except:
        print("mismatch")


    ret = ret.set_index(ids)
    average = ret.mean().mean()
    if average > 1:
        average = 2-average
    if average <.5:
        print("BAD P")

    assert average > .5, "A graded exam has an average score less than 50%"
    return ret


def create_classical_stats_from_graded(graded_df, form_name = "NAME_", get_P = True, get_N = True, get_pbis = True):
    # assumes id is index
    # assumes values of 1 and 0 or 1 and 2 where 2 is incorrect
    graded_df = graded_df.replace('2',0)
    graded_df = graded_df.replace('1',1)
    graded_df = graded_df.replace(2, 0)
    graded_df = graded_df.replace(1, 1)

    graded_df = graded_df.apply(pd.to_numeric, errors = 'coerce')
    graded_df['SCORE'] = graded_df.sum(axis = 1)
    a = graded_df.mean().astype(float)
    b = graded_df.corr()['SCORE']
    n = graded_df.count()
    ret = hfh.pd.DataFrame([])
    if get_pbis:
        ret[form_name + '_PBIS'] = b
        flagA = (b <.05) & (a <.9)
    if get_P:
        ret[form_name + '_P'] = a.astype('float')
        flagB = (a>.9) | (a<.35)
    if get_N:
        ret[form_name + '_N'] = n
        flagC = n > 30
    ret[form_name + 'FLAG_PBIS'] = flagA & flagC
    ret[form_name + 'FLAG_P'] = flagB & flagC
    ret = ret.drop(index = 'SCORE')
    #todo: add n to this analysis

    return ret


def create_forms_from_bank(project_path, operational = True, create_bank_L = False):
    bank_files = hfh.get_all_files(project_path+'/bank_files')
    print("creating forms")
    for file in bank_files:
        if not create_bank_L and file.find("BANK")>0:
            pass
        else:
            b_df = pd.read_excel(file, header = 0)
            cut = ""
            if create_bank_L and file.find('BANK')>-1:
                # check if passing is present
                i = file.find('_')
                if i > -1:
                    name = hfh.get_stem(file)
                    cut = name[i:]
                    try:
                        cut = int(cut)
                    except ValueError:
                        print(file,"contains and underscore but does not provide a cut")
                form = pd.DataFrame([])
                form['AccNum'] = b_df['AccNum']
                form_length = len(form)+1
                form.insert(0,'Position', range(1,form_length))

            elif operational:
                if 'UseCode' in b_df.columns:
                    form = b_df[b_df['UseCode']=='Operational']
                    form = form[['Position','AccNum']]
            elif 'Position' in b_df.columns:
                form = b_df[['Position', 'AccNum']]

            name = hfh.get_stem(file)
            suffix = '_LF'
            prefix = 'full'
            if operational:
                suffix = '_LO'+ str(cut)
                prefix = 'operational'
            form.to_csv(project_path+'/forms/'+prefix +'/'+name+suffix+'.csv', index = False)


def create_FINAL_from_CULLED(project_path):
    culled_path = hfh.get_all_files(project_path+ '/reports', target_string = 'CULLED.csv')
    cal_f_path = hfh.get_all_files(project_path + '/calibration', target_string = 'CAL_f.csv')
    cal_c_path = hfh.get_all_files(project_path + '/calibration', target_string = 'CAL_c.csv')
    if len(culled_path)*len(cal_f_path)*len(cal_c_path)== 1:
        f_df = hfh.get_df(cal_f_path[0])
        c_df = hfh.get_df(cal_c_path[0])
        #c_df.columns = ['AccNum','A','B','C','D','E']
        culled_df = hfh.get_df(culled_path[0], header = 0)
    else:
        print("invalid call CULLED, CAL_f and CAL_c")
        return False
    ids = f_df.iloc[:,0]
    f_df  = f_df.drop(columns = [0])
    f_df = f_df.T
    f_df = f_df.reset_index()
    f_df = f_df.drop(columns='index')
    c_df.columns = ['AccNum', 'B', 'C', 'D', 'E', 'F']
    f_df.insert(0,'AccNum', c_df['AccNum'])

    c_df = c_df[c_df['AccNum'].isin(culled_df['AccNum'])]
    f_df = f_df[f_df['AccNum'].isin(culled_df['AccNum'])]
    f_df = f_df.drop(columns=['AccNum'])
    f_df = f_df.T
    f_df.insert(0,'ID',ids)
    name = hfh.get_stem(cal_f_path[0])[:-6]
    path = project_path+'/calibration/'+name+'_FINAL_'
    f_df.to_csv(path+'f.csv',header = False, index = False)
    c_df.to_csv(path + 'c.csv', header=False, index=False)

    print('hello')


def parse_LXR_control(file_path):
    lines = hfh.get_lines(file_path)
    ret = []
    if lines is not False:
        for line in lines:
            line = line.strip()
            AccNum = ""
            period_i = line.find('.')
            if period_i > -1:
                #   look for a period, if present is of form:
                #       5.	HERBAL              851

                s = line.split()
                content = s[1].strip()
                number = s[2].strip()
                zeroes_needed = 3-len(number)
                number_string = ""
                for z in range(zeroes_needed):
                    number_string +='0'
                number_string += str(number)
                AccNum = content[:5] + number_string

            target_string = 'Key: '
            key_i = line.find(target_string)

            if key_i > -1:
                #   look for Key: , if present is of form:
                #       183 Form #: Feb 10     Key: A   ASSES367

                new_line = line[key_i+len(target_string)+2:].strip()
                s = new_line.split()
                # should only have 2 options
                if len(s)>2:
                    print("issue with parse_LXR_KEY")
                else:
                    if len(s) == 0:
                        print("problem")
                    if len(s) == 1:
                        AccNum = s[0].strip()
                    if len(s) == 2:
                        number = s[1].strip()
                        zeroes_needed = 3 - len(number)
                        number_string = ""
                        for z in range(zeroes_needed):
                            number_string += '0'
                        number_string += str(number)
                        AccNum = s[0].strip()+number_string
            s = line.split()
            if len(s) == 2:
                topic = s[0].strip()
                number = s[1].strip()
                AccNum = topic[:5]+number
            s = line.split(',')
            if len(s) == 2:
                topic = s[0].strip()
                number = s[1].strip()
                AccNum = topic[:5] + number
            s = line.split(',')
            if len(s) > 20:
                topic = s[0]
                number = s[1]
                result = topic + str(number)
                AccNum = result
            if len(line)>1 and AccNum == "":
                AccNum = line
            if len(AccNum) > 0:
                ret.append(AccNum)

        ret = hfh.pd.Series(ret)

        return ret
    else:
        return False


def create_c_from_LXR_Test(file_path, destination_path = None):
    if destination_path is None:
        destination_path = hfh.get_parent_folder(file_path)
    lines = hfh.get_lines(file_path)
    ret = []
    counter = 0
    for line in lines:
        counter += 1
        if line[0].isnumeric():
            entry = line.split()
            test_name = hfh.get_stem(file_path)
            test_id = line[:line.index('.')]
            entry[1]
            bank_id = entry[1] + '_' + entry[2]

            if len(entry) == 4:
                subject = entry[1] + "_" + entry[2]
                bank_id = subject + entry[3]
            key_line = lines[counter]
            key_i = key_line.find('Key: ')
            if key_i > -1:
                key = key_line[key_i + len("Key: ")]
            else:
                print("hello")
            record = [bank_id, key, '4', '1', 'Y', 'M']
            ret.append(record)
    df = pd.DataFrame(ret)

    name = hfh.get_stem(file_path) + "_c.csv"
    # df.sort_values(df[1])
    df.to_csv(destination_path + "/" + name, index=False, header=False)
    return df


def get_max_id_length(lines, verbose = True):
    #   potentially inefficient
    error_flag = False
    error_lines = []
    ret = 0
    csv_flag = False
    assert len(lines)>0, "asked for max id of empty file in get_max_id"
    for line in lines:
        s = line.split(',')
        if len(s)>1:
            csv_flag = True
        else:
            s = line.split()
        if len(s)>0:
            id = s[0]
            if len(id)>ret:
                ret = len(id)
    if error_flag and verbose:
        print("non-fatal error(s) detected in get_maximum_line_length")
        for line in error_lines:
            print (line)
        print("#___#")
    if verbose:
        print("CSV_FLAG = " + str(csv_flag))
        print(lines[0])
        if len(lines)>4:
            print(lines[4])
        print("+++++++++++++++++++")
    return ret


def set_id_length_on_line(line, id_length, comma_delimited = False):
    split = line.split(',')
    if len(split) <2:
        split = line.split()
    if len(split)>1:
        id = split[0]
        ret = ""
        if len(id) < id_length:
            filler_needed = id_length - len(id)
            for i in range(filler_needed):
                ret += '_'
            ret += id
        response = id + line[len(id)+1:]
        return ret + response

    print("line was asked to adjust id length but did not contain id and response:" + line)


def create_form_from_c(c_file, destination_path):
    c_df = hfh.get_df(c_file)
    form = hfh.pd.DataFrame([])
    form['AccNum'] = c_df[0]
    form['Sequence'] = form.index.values + 1
    form = form[['Sequence', 'AccNum']]
    name = hfh.get_stem(c_file)[:-2]+'_L.csv'
    form.to_csv(destination_path + '/' + name, header=True, index = False)

def process_LXR_key(key_file, get_c_df_AS_0 = False, get_L_df_AS_1 = False, destination_path_c = None, destination_path_L = None):
    c_df = None
    L_df = None
    name = hfh.get_stem(key_file)
    lines = hfh.get_lines(key_file)
    ids = []
    keys = []
    for line in lines:
        split = line.split('.')
        if len(split)>1:
            id = split[1]
            id = id.strip()
            id = id.replace(' ','_')
            ids.append(id)
        else:
            split = line.split(':')
            if len(split)>1:
                key = split[1]
                key = key.strip()
                keys.append(key)
    df = hfh.pd.DataFrame([ids, keys]).T
    df[2] = 4
    df[3] = 1
    df[4] = 'Y'
    df[5] = 'M'
    df[6] = df.index.values + 1

    c_df = df[[0,1,2,3,4,5]]
    L_df = df[[6,0]]

    if get_c_df_AS_0 or get_L_df_AS_1:
        return c_df, L_df
    if destination_path_c:
        c_df.to_csv(destination_path_c + '/' + name+'_c.csv', index = False, header = False)
    if destination_path_L:
        L_df.to_csv(destination_path_L + '/' + name + '_L.csv', index = False, header = False)

def get_operational_c_f(f_df, bank_df):
    mask = bank_df['UseCode'] == 'Operational'
    f_df = f_df.T
    f_df = f_df.set_index(bank_df['AccNum'], drop = True)
    b_df = bank_df[mask]
    f_df = f_df[mask]
    c_df = bank_df[['AccNum','Correct','Domain']]
    c_df['Include'] = 'Y'
    c_df['Options'] = 4
    c_df['Type'] = 'M'

    c_df = c_df[['AccNum','Correct','Domain','Options','Include','Type']]
    c_df.columns = ['AccNum','Key','Domain','Options','Include','Type']

    return c_df, f_df
#key_path = r"G:\LICENSING EXAMINATION\Shared\_IRT\PASSING_SCORE_IRT\LMLE\forms\LXR_KEY_FILES/LMLEJan2021Key.txt"
#process_LXR_key(key_path,destination_path_L=r'G:\LICENSING EXAMINATION\Shared\_IRT\PASSING_SCORE_IRT\LMLE\forms')

def get_strict_format_f_df(c_df, f_df, debug = True, get_c_df = False):
    # incoming
        # index = default
        # col 0 is id
        # header is default
    # output
        # index = id
        # columns = accNum

    # could add validation logic here
    if "AccNum" not in c_df.columns:
        c_df.columns = ['AccNum','Key','Options','Domain','Include','Type']
    test = 0 in f_df.columns
    assert test,"FATAL_ERROR: strict format assumes that f_df has a column 0 which contains id"
    f_df = f_df.set_index(f_df[0], drop=True)
    f_df = f_df.drop(columns=[0])
    f_df = f_df.T

    accNum = c_df['AccNum'].str.strip()

    f_df = f_df.set_index(accNum)
    f_df = f_df.T
    if get_c_df:
        return c_df, f_df
    return f_df

def strict_get_operational(c_df, f_df):
    # assumes strict formatting
    # assumes c_df is accurate (Y and N for include are correct)
    # assumes c_df has a header
    f_df = f_df.T
    c_df = c_df.set_index('AccNum')
    mask = c_df['Include'] =='Y'
    c_df = c_df[mask]
    f_df = f_df[mask]

    return c_df, f_df

def strict_grade(c_df, f_df, operational = True, debug = True, correct =1 , incorrect = 0):

    if operational:
        o_ret = strict_get_operational(c_df, f_df)
        if o_ret[0].shape[0] == c_df.shape[0] and debug:
            print("strict grade operational argument did not remove any items.")
        c_df = o_ret[0]
        ret_df = o_ret[1].T
    else:
        ret_df =  hfh.pd.DataFrame.copy(f_df)

    current_position = -1
    for answer in c_df['Key']:
        current_position += 1
        accNum = f_df.columns[current_position]
        ret_df.loc[ret_df[accNum] == answer, ret_df.columns[current_position]] = correct
        ret_df.loc[ret_df[accNum] != correct, ret_df.columns[current_position]] = incorrect

    ret = ret_df.apply(hfh.pd.to_numeric, errors = 'ignore')
    mean = ret.mean().mean()
    message = "strict grade found p of < .5. Mean = " + hfh.c_round(mean, as_string=True, as_percentage=True)
    if mean < .5:
        print(message)
        assert False
    return ret


def get_f_df_repeat_status(f_path):
    # places that repeat information lives...
    #   end of string
    #   pearson file type thrid column
    #   ... other things I have not come across
    ids_with_repeat_status = []
    lines = hfh.get_lines(f_path)
    if is_type_K(lines):
        df = hfh.get_df(f_path,header=0)
        df = df.drop(0)
        df['Attempt'] = df['Attempt'].replace(['1'],'F')
        df['Attempt'] = df['Attempt'].replace(['2'], 'R')
        ids_with_repeat_status = df['ClientID'] + '_' + df['Attempt']
    else:
        for line in lines:
            ending_character = line.strip()[-1]
            if ending_character in ['F', 'R']:
                repeat_status = ending_character
                line = line.strip()
                split_line = line.split()
                _id = None
                if len(split_line) > 1:
                    _id = split_line[0]
                else:
                    split_line = line.split(',')
                    if len(split_line)>1:
                        _id = split_line[0]
                if _id is None:
                    assert False, "can not assign repeat status to file " + f_path
                ids_with_repeat_status.append(_id+'_'+repeat_status)

    ret = process_response_string_file(f_path, create_c=False)
    f_df = ret
    f_df = f_df.set_index(ids_with_repeat_status)
    return ids_with_repeat_status


def get_header_argument(c_path):
    lines = hfh.get_lines(c_path)
    split = lines[0].split(',')
    if split[0] == 'AccNum': return 0
    return None


def is_strict_format(c_df, f_df):

    fx_name = "is_strict_format"
    # strict format _f:
    #   id as index
    #   todo: establish rule for missing data here

    assert f_df.shape[0] > 0, "empty f_df in " + fx_name
    assert 0 not in f_df.columns, "0 still present in f_df columns. This implies ID. Assertion in " + fx_name
    first_row_L = f_df.loc[0].to_list()
    print("hello")


    # strict format _c:
    #   csv
    #   has header
    #   does not have b values

    pass

def remove_accNum_from_f_and_c(accNum, name, program_path, reason = None):
    # create backup_processed_data folder
    report_folder = program_path + '/reports'
    backup_processed_folder = program_path+'/' + E.BACKUP_PROCESSED_DATA_P
    processed_folder = program_path + '/processed_data'
    hfh.create_dir(backup_processed_folder)
    # create notation of removal with reason
    f_df = hfh.get_single_file(processed_folder, target_string=name+'_f.csv', as_df=True, strict=True)
    c_file = hfh.get_single_file(processed_folder, target_string=name+'_c.csv', strict = True)
    c_df = hfh.get_df(c_file, header = get_header_argument(c_file))

    s_ret = get_strict_format_f_df(c_df, f_df, get_c_df=True)
    c_df = s_ret[0]
    f_df = s_ret[1].T
    f_df = f_df.drop(accNum)
    c_df = c_df.set_index(['AccNum'])
    c_df = c_df.drop(accNum)
    c_df = c_df.reset_index(drop=False)
    f_df = f_df.T
    strict_grade(c_df, f_df, operational=False) # solely for validation
    f_df.to_csv(program_path+'/processed_data/'+name+'_f.csv',header = None, index = True)
    c_df.to_csv(program_path+'/processed_data/'+name+'_c.csv', index = False, header = None)

    removed_report_path = hfh.get_all_files(program_path + "/" + E.REPORTS_P+'/', target_string=E.REMOVED_ITEMS_R)

    entry = accNum + " was removed from " + name
    if reason is not None:
        entry += " because of a " + reason
    if len(removed_report_path) == 0:
        removed_report_path = program_path + "/" + E.REPORTS_P + '/' + E.REMOVED_ITEMS_R
        hfh.write_lines_to_text([entry+'\n'],removed_report_path)
    else:
        hfh.add_lines_to_csv(removed_report_path[0], [entry])


def return_accNum_to_f_c(accNum, form, program_path):
    print("return accNum to f c has not been implemented in raw_processor")
    pass


def validate_c_file_header(c_file, debug = True):
    #first line should be AccNum...
    lines = hfh.get_lines(c_file)
    if debug:
        print("validating: " + c_file)
    assert len(lines)>0, "validate_c_file was fed an empty file"
    if lines[0] == E.C_HEADER_S + '\n':
        return True
    if lines[1] == E.C_HEADER_S + '\n':
        assert False, "validate_c_file detected 2 headers"
    #   header now present
    df = hfh.get_df(c_file)
    df.columns = E.C_HEADER_L
    df.to_csv(c_file,index = False)
    return True


def CAB_processor(project_files, tuples_start_operational_and_start_pretest, bank_path):
    print("CAB_processor is not implemented yet")
    assert type(project_files) == list, "project files must be a list"
    assert type(tuples_start_operational_and_start_pretest) == list, 'tuples_start... is a list of tuples'
    assert len(project_files) == len(tuples_start_operational_and_start_pretest), "list lengths must match"
    extension = hfh.get_extension(bank_path)
    assert extension == 'xlsx', "bank path must lead to a a.xlsx file"
    dfs = []
    for file in project_files:
        df = process_response_string_file(file)
        dfs.append(df)
    print("hello")

def CAB_simple_processor(path_to_raw_for_form, path_to_bank):
    project1 = hfh.get_single_file(path_to_raw_for_form, target_string="Project 1")
    project2 = hfh.get_single_file(path_to_raw_for_form, target_string="Project 2")
    general  = hfh.get_single_file(path_to_raw_for_form, target_string="General")
    p1_df = process_response_string_file(project1, get_df=True, create_c=False)
    p2_df = process_response_string_file(project2, get_df=True, create_c=False)
    p3_df = process_response_string_file(general, get_df=True, create_c=False)
    dfs = [p1_df,p2_df,p3_df]
    f_df = hfh.pd.concat(dfs, axis = 1)
    f_df = f_df.reset_index(drop=False)
    c_df = create_c_df_from_bank(bank_path)
    f_df = get_strict_format_f_df(c_df, f_df)

    g = strict_grade(c_df, f_df, operational=False)
    processed = "G:\LICENSING EXAMINATION\Shared\_IRT\PASSING_SCORE_IRT\CAB/processed_data/"
    f_df.to_csv(processed + "CAB20SEP_f.csv",header = False)
    c_df.to_csv(processed + "CAB20SEP_c.csv", header = False, index = False)


def parse_string_to_csv(string, print_string = False, return_string = False, return_series = False):
    if print is False and return_string is False and return_series is False:
        assert False, "parse string did not receive adequate instructions"
    if return_string is True and return_series is True:
        assert False, "parse string can not return both a string and a Series."

    ret = []
    ret_string = ""
    for i in string:
        ret_string += i + ','
        ret.append(i)
    ret_string = ret_string.strip(',')

    if print_string:
        print(ret_string)
    if return_string:
        return ret_string
    if return_series:
        return pd.Series(ret)

bank_path = r"G:\LICENSING EXAMINATION\Shared\_IRT\PASSING_SCORE_IRT\CAB\bank_files\CAB20SEP.xlsx"
path = r"G:\LICENSING EXAMINATION\Shared\_IRT\PASSING_SCORE_IRT\CAB\raw_data"
#CAB_simple_processor(path, bank_path)
s = "CBBACDAAAADAADDDBDCBDACDCADBDCCDDACDCACAADCBCADBBBDDDCDDBDCBDBBDDDABAABBACBABCCBCBCABADADABDDDBCABABCCBBBADABABABCDDCDCABAABDBABBCCADCDABBBADCAACBCCAC"
#parse_string_to_csv(s, print_string=True)