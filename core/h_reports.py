import core.h_passing_score as hp
import core.h_raw_processor as hr
import datetime as t

from numpy import nan
hfh = hp.hfh
E = hfh.E
pd = hfh.pd


def check_assumptions(search_folder, initial_or_final_s, bank_comparison = True, name = None, passing_rough = .7):
    if search_folder is not None:
        stats_path = hfh.get_all_file_names_in_folder(search_folder+'/xCalibre/'+initial_or_final_s,target_string='Stats.csv')
        tif_path = hfh.get_all_file_names_in_folder(search_folder+'/xCalibre/'+initial_or_final_s, target_string='TIF.csv')
        cal_f_path = hfh.get_all_file_names_in_folder(search_folder+'/calibration/'+initial_or_final_s, target_string='_f.csv')
        if bank_comparison:
            bank_path = hfh.get_all_file_names_in_folder(search_folder+'/bank_files', target_string='BANK.xlsx')
        if len(stats_path)>1 or len(tif_path)>1 or len(cal_f_path)>1:
            print("more than one file matches parameters or check_assumptions")
            return 0
        if bank_comparison:
            b_check = len(bank_path)
        else:
            b_check = 1
        if len(stats_path)*len(tif_path)*len(cal_f_path)*b_check==0:
            if len(stats_path) == 0:
                print("missing stats path")
            if len(tif_path) == 0:
                print("missing tif_path")
            if len(cal_f_path) == 0:
                print("missing cal_f")
            if len(bank_path) == 0:
                print("missing bank_path")

            return 0
        stats_path = stats_path[0]
        tif_path = tif_path[0]
        cal_f_path = cal_f_path[0]
        if bank_comparison:
            bank_path = bank_path[0]
            bank_df = pd.read_excel(bank_path, header = 0)
    if stats_path is None or tif_path is None or cal_f_path is None:
        print("missing required files/paths in check_assumptions")
        return 0
    stats_df = hfh.get_stats_df(stats_path, remove_na=False)
    tif_df = hfh.get_df(tif_path, header = 0)
    matrix_df = hfh.get_df(cal_f_path)
    cols = matrix_df.columns[1:]
    last_column = cols[len(cols)-1]
    matrix_df = matrix_df.drop(columns=[last_column])
    matrix_df.columns = cols
    passing_rough = .7
    passing_theta = hp.get_theta_from_percent_correct(passing_rough,tif_df = tif_df, return_stem=False)
    fit_df = evaluate_in_fit_out_fit(stats_df)
    info_df = evaluate_max_info(stats_df,tif_df,passing_rough)
    #std_resid_df = hs.get_residuals(matrix_df=matrix_df,stats_df=stats_df, tif_df = tif_df, convert_stats_df_to_binary=True)
    '''
    #resid_ret[1].to_csv("VMB_resids.csv")
    #resid_ret[2].to_csv("VMB_std_resids.csv")
    #std_resid_df = hfh.get_df('VMB_STD_RESID.csv', header = 0,dtype = 'float')
    #means = std_resid_df.mean(axis = 0)
    #high_resid = pd.DataFrame([])
    #for column in std_resid_df.columns:
        #df['new column name'] = df['column name'].apply(lambda x: 'value if condition is met' if x condition else 'value if condition is not met')
        #converter = lambda x : x*2 if x < 10 else (x*3 if x < 20 else x)
        high_resid[column] = std_resid_df[column].apply(lambda x: nan if pd.isnull(x) else (1 if abs(x)>5 else 0))
    high_resid = high_resid.T
    high_resid['Total'] = high_resid.sum(axis = 1)
    high_resid['N'] = high_resid.count(axis = 1)
    high_resid['BAD_FIT_PROB'] = high_resid['Total']/high_resid['N']
    '''
    stats_df.index = stats_df['Item ID']
    final_df = fit_df[['In-MSQ','Out-MSQ','z Resid','IN_DEV_EVAL','OUT_DEV_EVAL','Z_RESID_EVAL','B_EVAL']].copy()
    final_df.index = fit_df['AccNum']
    final_df.loc[:,'b'] = stats_df['b']
    final_df.loc[:,'POINT_BISERIAL_S'] = stats_df['S-Rpbis']
    final_df.loc[:,'POINT_BISERIAL_T'] = stats_df['T-Rpbis']
    final_df.loc[:,'N'] = stats_df['N']
    final_df = final_df.astype(float)
    #final_df.loc[:,'Z_RESID_EVAL'] = final_df['z Resid'].apply(lambda x: 1 if abs(x)<2 else 0)
    final_df.loc[:,'TPBIS_EVAL'] = final_df['POINT_BISERIAL_T'].apply(lambda x: -1 if x < .1 else 1)
    final_df.loc[final_df['POINT_BISERIAL_S'] < .05, 'TPBIS_EVAL'] = -10

    info_df.index = final_df.index
    final_df['THETA_DELTA_FROM_CUT'] = info_df
    #todo: consider arbitrary B cutoff
    B_CUTOFF = 1.8
    final_df['B_EVAL'] = final_df['THETA_DELTA_FROM_CUT'].apply(lambda x: 1 if abs(x)<B_CUTOFF else 0)
    #final_df['N'] = high_resid['N']
    final_df['ITEM_ASSUMPTION_SCORE'] = final_df['IN_DEV_EVAL'] + final_df['OUT_DEV_EVAL'] + final_df['B_EVAL'] + final_df['Z_RESID_EVAL']+final_df['TPBIS_EVAL']+1
    final_df.loc[final_df['ITEM_ASSUMPTION_SCORE'] <1 , 'ITEM_ASSUMPTION_SCORE'] = 0
    final_df['ITEM_IRT_QUALITY'] = final_df['ITEM_ASSUMPTION_SCORE'].apply(lambda x: 0 if x==0 else 1)


    #final_df['BAD_FIT_PROB'] = high_resid['BAD_FIT_PROB']
    #todo evaluate but seems to be a product of difficulty
    count = final_df.groupby('ITEM_ASSUMPTION_SCORE', axis = 0).count()
    count = count['In-MSQ']
    summary = final_df.groupby('ITEM_ASSUMPTION_SCORE', axis = 0).mean()
    summary['items_at_this_score'] = count
    if bank_comparison:
        domain_info = bank_df['Domain']
        task_info = bank_df['Task']
        domain_info.index = bank_df['AccNum']

        task_info.index = bank_df['AccNum']
        final_df['Domain'] = domain_info
        final_df['Task'] = task_info
        culled_list = final_df[final_df['ITEM_IRT_QUALITY'] == 1]
        culled_list = culled_list.sort_values(['Domain','Task','ITEM_ASSUMPTION_SCORE'], ascending=[1,1,0])
    else:
        culled_list = final_df[final_df['ITEM_IRT_QUALITY'] == 1]


    calibration_path = search_folder + '/calibration/' + initial_or_final_s + '/'
    reports_path = search_folder + '/reports/'
    if name is None:
        base_name = hfh.get_stem(cal_f_path)[:-2]
    else:
        base_name = name
    summary.to_csv( reports_path + base_name + "_ASSUMPTION_SUMMARY.csv")
    culled_list.to_csv(calibration_path + base_name + "_CULLED.csv")
    final_df.to_csv(calibration_path + base_name + "_COMPLETE.csv")


def update_removed_item_report_from_culled_and_complete(culled_df = None, complete_df = None, program_path = None):
    if culled_df is None and complete_df is None and program_path is None:
        assert False, "update removed items requires either a culled and complete df or a program path"
    if program_path is None and (culled_df is None or complete_df is None):
        assert False, "since no program path was provided both culled and complete must be"
    if program_path is not None:
        culled_path = hfh.get_single_file(program_path + '/' + E.FINAL_CALIBRATION_P, target_string=E.CULLED_ITEMS_R)
        complete_path = hfh.get_single_file(program_path + '/' + E.INITIAL_CALIBRATION_P, target_string=E.COMPLETE_ITEMS_R)
        culled_df = hfh.get_df(culled_path, header = 0, index_col=0)
        complete_df = hfh.get_df(complete_path, header = 0, index_col=0)
    removed_items = complete_df[~complete_df.isin(culled_df)].dropna()
    removed_items = removed_items.reset_index(drop=False)
    removed_items = removed_items[['AccNum','TPBIS_EVAL','IN_DEV_EVAL','OUT_DEV_EVAL','B_EVAL','b']]
    removed_items['TPBIS_EVAL'] = removed_items['TPBIS_EVAL'].replace('-10','Negative point biserial')
    removed_items['TPBIS_EVAL'] = removed_items['TPBIS_EVAL'].replace('-1.0', 'Low point biserial')
    removed_items['TPBIS_EVAL'] = removed_items['TPBIS_EVAL'].replace('1.0', '')
    removed_items['IN_DEV_EVAL'] = removed_items['IN_DEV_EVAL'].replace('-2.0', 'Major In-fit Violation')
    removed_items['IN_DEV_EVAL'] = removed_items['IN_DEV_EVAL'].replace('-1.0', 'Minor In-fit Violation')
    removed_items['IN_DEV_EVAL'] = removed_items['IN_DEV_EVAL'].replace('1.0', '')

    removed_items['OUT_DEV_EVAL'] = removed_items['OUT_DEV_EVAL'].replace('-2.0', 'Major OUT-fit Violation')
    removed_items['OUT_DEV_EVAL'] = removed_items['OUT_DEV_EVAL'].replace('-1.0', 'Minor OUT-fit Violation')
    removed_items['OUT_DEV_EVAL'] = removed_items['OUT_DEV_EVAL'].replace('1.0', '')

    removed_items['B_EVAL'] = removed_items['B_EVAL'].replace('0', 'low utility difficulty, b = ')
    removed_items['B_EVAL'] = removed_items['B_EVAL'].replace('1', 'b =')

    list = removed_items.values.tolist()
    lines = []
    for entry in list:
        line = ""
        for item in entry:
            line += item + ' '
        line = line.strip()
        lines.append(line)
    path_to_removed = program_path + '/' + E.REPORTS_P + '/' + E.REMOVED_ITEMS_R
    if hfh.os.path.isfile(path_to_removed):
        hfh.add_lines_to_csv(lines = lines, path = program_path + '/' + E.REPORTS_P + '/' + E.REMOVED_ITEMS_R)
    else:
        hfh.write_lines_to_text(lines,path_to_removed, mode = 'a')


def evaluate_in_fit_out_fit(stats_df):
    df = stats_df

    #   check that file is 2 parameter IIF
    eval = pd.DataFrame()
    if "In-MSQ" in df.columns and "Out-MSQ" in df.columns:
        df = stats_df[['In-MSQ', 'Out-MSQ', 'b', 'z Resid']]
        df = df.apply(pd.to_numeric, errors='coerce')
        df = df.dropna(axis=0)

        df['B_EVAL'] = 0
        df['IN_DEV'] = abs(1 - df['In-MSQ'])
        df['OUT_DEV'] = abs(1 - df['Out-MSQ'])
        df.loc[df['IN_DEV'] > .5, 'IN_DEV_EVAL'] = -2
        df.loc[df['IN_DEV'] < .5, 'IN_DEV_EVAL'] = -1
        df.loc[df['IN_DEV'] < .2, 'IN_DEV_EVAL'] = 1

        df.loc[df['OUT_DEV'] > .5, 'OUT_DEV_EVAL'] = -2
        df.loc[df['OUT_DEV'] < .5, 'OUT_DEV_EVAL'] = -1
        df.loc[df['OUT_DEV'] < .2, 'OUT_DEV_EVAL'] = 1

        df.loc[df['z Resid'] > 2, 'Z_RESID_EVAL'] = 0
        df.loc[df['OUT_DEV'] <= 2, 'Z_RESID_EVAL'] = 1


        df['B_EVAL'] = 1
        df.loc[abs(df['b']) > 3, 'B_EVAL'] = -2

        df['AccNum'] = stats_df["Item ID"]
        df['Domain'] = stats_df['Domain']

        return df
    else:
        print("It appears you asked for a Rasch analysis of fit for a non Rasch model")


def evaluate_max_info(stats_df, tif, passing_proportion):
    # get theta for passing
    theta = float(tif.iloc[(tif['TRF'].astype(float) - passing_proportion).abs().argsort()[:1]]['Theta'].values[0])
    #   returns distance between max and passing as series
    work_df = stats_df
    work_df = stats_df.apply(pd.to_numeric, errors='coerce')
    work_df = work_df[work_df['Theta at Max'].notna()]
    stats_df['Theta_delta'] = work_df['Theta at Max'].astype(float)-theta
    #stats_df['Theta_percent'] = s.get_percent_from_theta(stats_df['Theta at Max'].astype(float),"no path", tif_df=tif)
    return stats_df['Theta_delta']


def evaluate_item_discrimination(stats_df):
    # df = stats_df
    df = pd.DataFrame()
    if "In-MSQ" in df.columns and "Out-MSQ" in df.columns:
        print("It appears you asked for a 2 parameter analysis discrimination but fed it Rasch raw_data.")
    else:
        df['Domain'] = stats_df['Domain']
        df['A'] = stats_df['a']
        df = df.apply(pd.to_numeric, errors='coerce')
        df = df.dropna(axis=0)
        df['A_DEV'] = abs(1 - df['A'])

        df.loc[df['A_DEV'] > .5, 'A_DEV_EVAL'] = -2
        df.loc[df['A_DEV'] < .5, 'A_DEV_EVAL'] = -1
        df.loc[df['A_DEV'] < .2, 'A_DEV_EVAL'] = 1

    return df


def identify_unsanctioned_items(project_directory, form, verbose = True):
    culled_path = hfh.get_all_file_names_in_folder(project_directory+'/reports/', target_string='CULLED')[0]
    culled_df = hfh.get_df(culled_path, header = 0)
    form_df = hfh.get_df(form, header = 0)
    ret = form_df[~form_df['AccNum'].isin(culled_df['AccNum'])].sort_values(by = ['AccNum'])
    if len(ret)>0:
        print(hfh.get_stem(form), ' contains ', str(len(ret)),'unsanctioned items')
        rep_str = ""
        counter = -1
        for i in ret['AccNum']:
            counter += 1
            rep_str += i + ','
            if (counter+1)%4 == 0 and counter > 0:
                rep_str = rep_str.strip(',') + '\n'
        print(rep_str)



    return ret['AccNum'].tolist()


def get_angoff_passing_report(project, form, angoff = None, strict = True):

    if angoff is None:
        form = hfh.get_stem(form)
        u = form.rfind('_')
        name = hfh.get_stem(form)[:u]
        angoff = int(form[u+1:])
    else:
        name = hfh.get_stem(form)[:-2]

    processed_path = project + '/backup_processed_data/'

    f_path = processed_path + name + '_f.csv'
    c_path = processed_path + name + '_c.csv'
    if hfh.os.path.isfile(f_path) is False or hfh.os.path.isfile(c_path) is False:
        print("missing _f or _c for form " + form)
        return False
    f_df = hfh.get_df(f_path, header=None)
    if strict:
        c_df = hfh.get_df(c_path, header=0)
    else:
        c_df = hfh.get_df(c_path, header=None)
    graded = hr.grade_examination(f_df, c_df, correct=1, incorrect=0, only_operational=True, grading_processed=True)
    ret = pd.DataFrame([])
    ret['SCORE'] = graded.sum(axis = 1)
    p_item = graded.mean()
    operational_items = graded.shape[1]
    ret['PERCENT_SCORE'] = ret['SCORE']/operational_items
    #cut_p = hp.create_passing(project,form,cut_theta, verbose=False)
    #if cut_p >=1: cut_p/=100
    ret['ANGOFF_CUT_SCORE'] = angoff
    ret['ANGOFF_CUT_P'] = angoff/operational_items

    ret['ANGOFF_PASS_RATE']  = 0
    ret.loc[ret['SCORE'] >= angoff, 'ANGOFF_PASS_RATE'] = 1
    ret = ret.mean()
    form_S = hfh.pd.Series([name],index=['FORM'])
    ret = form_S.append(ret)
    return ret, graded


def get_Rasch_passing_report(project, cut_theta, form, graded):
    ret = pd.DataFrame([])
    cut_p = hp.create_passing(project, form, cut_theta, verbose=True)/100
    operational_items = graded.shape[1]
    rasch_cut_score = int(cut_p * operational_items)
    n_score = 'SCORE'
    n_r_pass_rate = 'RASCH_' + str(cut_theta)+"_PASS_RATE"
    n_r_p = 'RASCH_CUT_P_' + str(cut_theta)
    n_r_c = 'RASCH_CUT_SCORE_' + str(cut_theta)
    ret[n_score] = graded.sum(axis = 1)
    ret[n_r_pass_rate] = 0
    ret.loc[ret[n_score] >= rasch_cut_score, n_r_pass_rate] = 1
    ret[n_r_c] = int(rasch_cut_score)
    ret[n_r_p] = cut_p
    ret = ret.mean()
    ret = ret.drop(n_score)
    return ret


def get_passing_report(project, rasch_thetas, forms = None, strict = False, debug = True):
    if forms is None:
        forms = hfh.get_all_files(project + '/' + E.OPERATIONAL_FORMS_P)
    if strict and debug:
        print("strict guarantees c_df has header.")
    report = []
    for form in forms:
        print("processing Angoff for " + form)
        form_ret = []
        print("not using drift... change me!!!!!")
        angoff_result = get_angoff_passing_report(project, form, angoff=None, strict = strict)
        graded = angoff_result[1]
        form_ret.append(angoff_result[0])
        for r in rasch_thetas:
            print("processing Rasch theta of " + str(r))
            rasch_ret = get_Rasch_passing_report(project,r,form,graded)
            form_ret.append(rasch_ret)

        form_ret = hfh.pd.concat(form_ret)
        report.append(form_ret)
    final = hfh.pd.DataFrame(report).T
    return final


def create_passing_comparison(project, cut_theta, form, angoff = None):
    if not angoff:
        angoff_cut_score = int(hfh.get_stem(form[form.find('_L')+1:]))
    else:
        angoff_cut_score = angoff

    if angoff is None:
        name = hfh.get_stem(form)[:-4]
    else:
        name = hfh.get_stem(form)[:-2]
    #todo make a canonical path for processed prior to drift removal

    processed_path = project + '/backup_processed_data/'
    f_path = processed_path + name + '_f.csv'
    c_path = processed_path + name + '_c.csv'
    if hfh.os.path.isfile(f_path) is False or hfh.os.path.isfile(c_path) is False:
        print("missing _f or _c for form " + form)
        return False
    f_df = hfh.get_df(f_path, header = None)
    c_df = hfh.get_df(c_path, header = None)
    #f_df.index = f_df.iloc[:,0]
    #f_df = f_df.drop(columns = 'ID')

    graded = hr.grade_examination(f_df, c_df, correct=1, incorrect=0, only_operational=True, grading_processed=True)
    ret = pd.DataFrame([])
    ret['SCORE'] = graded.sum(axis = 1)
    p_item = graded.mean()
    operational_items = graded.shape[1]
    ret['PERCENT_SCORE'] = ret['SCORE']/operational_items
    cut_p = hp.create_passing(project,form,cut_theta, verbose=False)
    if cut_p >=1: cut_p/=100

    ret['ANGOFF_CUT_P'] = angoff_cut_score/operational_items
    rasch_cut_score = int(cut_p*operational_items)
    ret['ANGOFF_PASS']  = 0
    ret.loc[ret['SCORE'] >= angoff_cut_score, 'ANGOFF_PASS'] = 1
    ret['RASCH_CUT_P_'+str(cut_theta)] = cut_p
    ret['RASCH_PASS_' + str(cut_theta)] = 0
    ret.loc[ret['SCORE'] >= rasch_cut_score, 'RASCH_PASS_' + str(cut_theta)] = 1
    ret = ret.mean()
    ret.loc['FORM']=hfh.get_stem(f_path)[:-2]
    ret.loc['RASCH_CUT_SCORE_'+str(cut_theta)] = rasch_cut_score
    ret.loc['ANGOFF_CUT_SCORE'] = angoff_cut_score
    return ret


def get_classical_stats(project):
    forms = hfh.get_all_file_names_in_folder(project+'/forms/archival_forms')
    ret = hfh.pd.DataFrame([])
    for form in forms:
        name = hfh.get_stem(form)[:-2]
        processed_path = project + '/processed_data/'
        f_path = processed_path + name + '_f.csv'
        c_path = processed_path + name + '_c.csv'
        if hfh.os.path.isfile(f_path) is False or hfh.os.path.isfile(c_path) is False:
            print("missing _f or _c for form " + form)
            return False
        f_df = hfh.get_df(f_path, header = None)
        c_df = hfh.get_df(c_path, header = None)
        #f_df.index = f_df.iloc[:,0]
        #f_df = f_df.drop(columns = 'ID')

        graded = hr.grade_examination(f_df, c_df, correct=1, incorrect=0, only_operational=True, grading_processed=True)
        classical_stats = hr.create_classical_stats_from_graded(graded, form_name=hfh.get_stem(form)[:-2]+'_')
        ret = hfh.pd.concat([ret,classical_stats],axis=1)
    return ret


def get_summative_week_stats(weekly_report_folder):
    #   assumes that a weekly report folder contains
    #       _REPORTS
    #       FORM_XLSX
    #       RESPONSE_STRINGS
    #

    report_folder_name = '_REPORTS'
    report_folder = weekly_report_folder+'/'+report_folder_name
    form_folder_name = 'FORM_XLSX'
    form_folder = weekly_report_folder + '/' + form_folder_name
    response_folder_name = 'RESPONSE_STRINGS'
    response_folder = weekly_report_folder + '/' + response_folder_name
    processed_folder_name = '_PROCESSED_DATA'
    processed_folder = weekly_report_folder + '/' + processed_folder_name

    date = t.date
    log_path = weekly_report_folder+'/ERROR_LOG'+'_'+date+'.txt'

    check_folder(report_folder, log_path)
    check_folder(form_folder, log_path)
    check_folder(response_folder, log_path)
    check_folder(processed_folder, log_path)

    response_files = hfh.get_all_file_names_in_folder(response_folder)
    forms = hfh.get_all_file_names_in_folder(form_folder)

    pairs = hfh.pair_files(response_files, forms)
    for pair in pairs:
        full_name = hfh.get_stem(pair[0])
        _i = full_name.find('_')
        date = full_name[_i:]
        name = full_name[:_i]
        response_file = pair[0]
        form = pair[1]
        f_df = hr.process_response_strings_for_IRT(response_file, processed_folder, form_folder, get_f_df=True)
        c_df = hr.create_c_df_from_bank(form)
        c_df.to_csv(processed_folder+'/')
        #todo: run stats on it


def check_folder(path, log_path):
    log = []
    if not hfh.os.path.isdir(path, log_path):
        log.append('BEGIN_ENTRY' + t.datetime)
        log.append(path + " does not exist.")
        created = hfh.os.mkdir(path)
        if created:
            log.append(path + ' created.\n')
        else:
            log.append(path + " does not exist and can not be created automatically. This is a fatal error.")
        log.append('END_ENTRY' + t.datetime)
        hfh.write_lines_to_text(log, log_path, mode='a')
        return 0

#test_path = r"G:\LICENSING EXAMINATION\Shared\_IRT\PASSING_SCORE_IRT\LMLE"
#update_removed_item_report_from_culled_and_complete(program_path=test_path)
