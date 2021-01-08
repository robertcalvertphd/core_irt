import  core.h_raw_processor as hr
import core.h_stats as hs
hfh = hr.hfh
math = hfh.math

# input
#   files are in base formatting _f _c
#   file names match X_f and X_c

# output
#   a drift report (to be developed)

def create_dfs(path):
    dfs = []
    path = path + '/processed_data'
    f_files = hfh.get_all_files(path, target_string='_f')
    c_files = hfh.get_all_files(path, target_string='_c')
    pairs = hfh.pair_files(c_files, f_files, pair_full=True)
    for pair in pairs:
        print(pair)
        name = hfh.get_stem(pair[0])[:-2]
        _header = hr.get_header_argument(pair[0])
        c_df = hfh.get_df(pair[0], header = _header)
        f_df = hfh.get_df(pair[1])
        f_df = hr.get_strict_format_f_df(c_df, f_df)
        g_df = hr.strict_grade(c_df, f_df, operational=False)
        n = g_df.shape[0]
        means = g_df.mean()

        #name_S = hfh.pd.Series(name,['NAME'])
        #g_df = name_S.append(g_df)
        df = hfh.pd.DataFrame([means],[name])
        df['N'] = n
        if n > 30:
            dfs.append(df)
    ret = hfh.pd.concat(dfs)
    return ret


def create_aggregate_descriptives(df):
    N_s = df['N']
    mean = df.mean()
    max = df.max()
    min = df.min()
    range = max-min
    df = hfh.pd.DataFrame([mean,max,min,range], index=['MEAN',"MAX","MIN","RANGE"])
    df = df.T
    return df


def get_drifting_items(df):
    issue_items = []
    for item in df.columns:
        s = df[item]
        ns = df['N']
        test_df = hfh.pd.DataFrame([s,ns]).T
        _min = test_df[test_df[item] == test_df[item].min()]
        min_p = _min.iloc[0,0]
        min_n = _min.iloc[0,1]
        min_form = _min.first_valid_index()
        min_pos = min_p*min_n
        min_neg = min_n - min_pos
        _max = test_df[test_df[item] == test_df[item].max()]
        max_p = _max.iloc[0,0]
        max_n = _max.iloc[0,1]
        max_pos = max_p * max_n
        max_neg = max_n - max_pos
        max_form = _max.first_valid_index()
        chi_ret = hs.chisq_2_2(min_pos,min_neg,max_pos,max_neg)
        alpha = chi_ret[1]
        chi2 = chi_ret[0]
        n = max_n + min_n
        phi = math.pow(chi2/n,.5)
        if phi > .2: #todo: change back to .15 lest you be confused
            print("phi is set at .2 change me!!!!!")

            mean = test_df.mean()
            mean_pos = mean[0] * ns.sum()
            mean_neg = ns.sum() - mean_pos

            alpha_max = hs.chisq_2_2(mean_pos-max_pos, mean_neg-max_neg, max_pos, max_neg)[1]
            alpha_min = hs.chisq_2_2(mean_pos-min_pos, mean_neg-min_neg, min_pos, min_neg)[1]
            score = 0
            note = ""

            if alpha_min < .01 < alpha_max:
                score = -1
                note = "MIN score is significantly lower than mean"
            if alpha_max < .01 < alpha_min:
                score = -1
                note = "MAX score is significantly greater than mean"
            if alpha_max < .01 > alpha_min:
                score = -2
                note = "Both max and min are significantly different than mean"
            range = max_p - min_p

            issue_items.append([item, min_form, max_form,min_p, min_n, max_p, max_n, alpha, alpha_max, alpha_min, score, range, phi, note])

    ret = hfh.pd.DataFrame(issue_items, columns = [
        "AccNum", "MIN_FORM", "MAX_FORM","MIN_P","MIN_N","MAX_P","MAX_N","X^2_alpha","X^2_alpha_max","X^2_alpha_min","drift_score","RANGE","PHI","Note"])
    ret = ret.set_index(['AccNum'])

    return ret


def X_remove_drift_items_from_calibrated(path, drift_df, write_csv = False):
    # this is overkill... instead perhaps we should simply update to most recent stats

    c_df = hfh.get_single_file(path+'/calibration/initial', target_string='_c', as_df=True)
    f_df = hfh.get_single_file(path + '/calibration/initial', target_string='_f', as_df=True)
    f_df = hr.get_strict_format_f_df(c_df, f_df)
    c_df = c_df.set_index('AccNum')
    list_of_values = drift_df.index.values
    drop_items = c_df.loc[drift_df.index.values]
    f_df = f_df.T
    f_df = f_df.drop(drop_items.index.values)
    c_df = c_df.drop(drop_items.index.values)
    f_df = f_df.T
    if write_csv:
        f_df.to_csv(path+"/calibration/initial/TEST_LMLE_f.csv", header = False)
        c_df.to_csv(path+"/calibration/initial/TEST_LMLE_c.csv", header = False)
    return c_df, f_df

def X_remove_accNum_from_f_and_c(accNum, name, program_path, reason = 'DRIFT'):
    report_folder = program_path + '/reports'
    drift_folder = program_path+'/processed_with_drift_present'
    processed_folder = program_path + '/processed_data'
    hfh.create_dir(drift_folder)
    # create notation of removal with reason
    f_df = hfh.get_single_file(processed_folder, target_string=name+'_f.csv', as_df=True, strict=True)
    c_file = hfh.get_single_file(processed_folder, target_string=name+'_c.csv', strict = True)
    c_df = hfh.get_df(c_file, header = hr.get_header_argument(c_file))
    f_df.to_csv(drift_folder + '/' + name + '_f.csv', header=None, index=False)
    c_df.to_csv(drift_folder + '/' + name + '_c.csv', header = 0, index=False)


    s_ret = hr.get_strict_format_f_df(c_df, f_df, get_c_df=True)
    c_df = s_ret[0]
    f_df = s_ret[1].T
    f_df = f_df.drop(accNum)
    c_df = c_df.set_index(['AccNum'])
    c_df = c_df.drop(accNum)
    c_df = c_df.reset_index(drop=False)
    f_df = f_df.T
    hr.strict_grade(c_df, f_df, operational=False) # solely for validation
    f_df.to_csv(program_path+'/processed_data/'+name+'_f.csv',header = None, index = True)
    c_df.to_csv(program_path+'/processed_data/'+name+'_c.csv', index = False)

def remove_low_from_f_c(drift_df, program_path):
    to_be_removed_t = drift_df['MIN_FORM'].iteritems()
    for i in to_be_removed_t:
        hr.remove_accNum_from_f_and_c(i[0],i[1],program_path=program_path, reason="DRIFT VIOLATION")



def populate_backup_processed_data(program_path):
    files = hfh.get_all_files(program_path + '/' + hfh.E.PROCESSED_DATA_P)
    for file in files:
        extension = hfh.get_extension(file)
        if extension == 'zip':
            print("zip found and ignored")
        else:
            name = hfh.get_name(file)
            path = program_path + '/' + hfh.E.BACKUP_PROCESSED_DATA_P
            full_path = path + '/' + name
            df = hfh.get_df(file)
            df.to_csv(full_path, header = False, index = False)


def remove_high_from_drifted(drifted_df, program_path):
    processed_path = program_path + '/processed_data'
    assert False, "not implemented yet"
    # get a series for each item in drifted


def remove_low_from_drifted(drifted_df):
    assert False, "not implemented yet"


# path = r"C:\Users\ERRCALV\PycharmProjects\IRT_RCC\IRT_PASSING\CAB"
# name = 'CAB18SEP'
# accNum = 'CAB189'
# remove_accNum_from_f_and_c(accNum,name,path)


def run(path, name, debug = True):
    # copy original files to backup_processed_files
    populate_backup_processed_data(path)
    if debug:
        print("drift analysis creating dfs")
    df = create_dfs(path)
    if debug:
        print("drift analysis getting drifted items")
    ct = get_drifting_items(df)
    drift_df = remove_low_from_f_c(ct, path)
    if debug:
        print("removing drifted items from _f and _c")

    ad = create_aggregate_descriptives(df)
    df.to_csv(path+'/reports/' + name + '_longitudinal_item_stats.csv')
    ad.to_csv(path+'/reports/' + name + '_aggregate_descriptives.csv')
    ct.to_csv(path+'/reports/' + name + '_drift_violations.csv')

    #   remove_drift_items_from_calibrated(path, ct, write_csv=True)
    #   print("hello")


#run(path = r"C:\Users\ERRCALV\PycharmProjects\IRT_RCC\IRT_PASSING\CAB")



