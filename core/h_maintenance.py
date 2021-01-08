import core.h_file_handling as hfh
import core.h_raw_processor as hr
from numpy import nan


def update_control(new_df, cal_df, cal_stats):
    cal_df = cal_df.rename(columns={0:'AccNum',4:'Include'})
    new_df = new_df.rename(columns={0: 'AccNum', 4: 'Include'})

    stats = hfh.get_stats_df(stats_path=cal_stats, remove_na=False)
    Bs = stats[['Item ID','b']]
    Bs = Bs.set_index(Bs['Item ID'])
    Bs = Bs.drop(columns = 'Item ID')

    cal_df['Include'] = 'A'
    cal_df = cal_df.set_index(cal_df['AccNum'])

    new_items = new_df[~new_df['AccNum'].isin(cal_df['AccNum'])]
    new_items = new_items.drop(columns=['AccNum'])
    cal_df = cal_df.drop(columns=['AccNum'])
    cal_df['b'] = nan
    cal_df['b'] = Bs['b']
    new_items['b'] = nan

    ret = hfh.pd.concat([cal_df,new_items])
    m = ret['b'].isnull()
    ret.loc[m, 'Include'] = 'Y'
    ret[1] = 1
    ret[2] = 2

    return ret


def update_formatted(combined_c_df, new_f_df, new_c_df, old_f_df, old_c_df):
    #loop through control_df for each AccNum
    graded = hr.grade_examination(new_f_df, new_c_df)
    new = hfh.pd.DataFrame([],columns=combined_c_df.index)

    for item in graded:
        if item in new.columns:
            new[item] = graded[item]

    ret = hfh.pd.concat([old_f_df,new])
    return ret


def add_new_form_to_data(new_c_path, new_f_path, calibrated_c_path, calibrated_f_path, calibrated_stats):
    checking = True

    new_f_df = hfh.get_df(new_f_path)
    new_c_df = hfh.get_df(new_c_path)
    cal_f_df = hfh.get_df(calibrated_f_path)
    cal_c_df = hfh.get_df(calibrated_c_path)
    c_ids = cal_f_df.iloc[:,0]
    n_ids = new_f_df.iloc[:,0]


    combined_c = update_control(new_c_df, cal_c_df, calibrated_stats)
    combined_f = update_formatted(combined_c,new_f_df,new_c_df, cal_f_df, cal_c_df)
    if checking:
        check = cal_f_df.drop(columns=0)
        check = check.apply(hfh.pd.to_numeric, errors='coerce')
        check = check.replace(2.0, 0.0)
        check['SCORE'] = check.sum(axis=1)
        check['ID'] = c_ids
        check_it = check[['ID', 'SCORE']]

        check = combined_f.drop(columns=0)
        check = check.apply(hfh.pd.to_numeric, errors='coerce')
        check = check.replace(2.0, 0.0)
        check['SCORE'] = check.sum(axis=1)
        check_it[['SCORE_2']] = check['SCORE']
        test = check_it['SCORE_2'] != check_it['SCORE']
        check_it = check_it[test]
        if len(check_it)>0:
            print("ERROR IN COMBINATION!!!!")
            return False
    combined_f.to_csv("COMBINED_F.csv", index = False, header = None)
    combined_c.to_csv("COMBINED_C.csv", index = True, header = None)
