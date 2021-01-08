import core.h_raw_processor as hr
hfh = hr.hfh
from math import pow
import datetime

import core.h_reports as rep

#   PURPOSE
#       process a series of response strings into a single _f file
#       pair the _f file with a _c file which is created from a bank file.
#       name the files appropriately

#   ENVIRONMENT REQUIREMENTS
#       folder name is canonical bank name
#           LEX20SEP or LEX20SEP_A
#       all txt files in folder are response strings
#       there is exactly 1 xlsx bank file in the folder

#   OUTPUT
#       /processed_data
#           _f file
#           _c file
#       /reports
#           date_10.csv
#               classical stats
#           date_10_flags.csv
#               flagged items for negative pbis or extreme P

#   HOOK
#       create_10_day_stats

def create_processed(project_path):
    d = datetime.datetime.today()
    day = str(d.day)
    month = str(d.month)
    year = str(d.year)
    #   move everything that is not in a folder to raw_data
    #   validate the contents of raw data
    #   process raw data to _c and _f
    response_files = hfh.get_all_files(project_path, extension='txt')
    bank_files = hfh.get_all_files(project_path, extension= 'xlsx')
    assert len(bank_files) == 1, "THERE MUST BE EXACTLY ONE BANK FILE IN " + project_path

    f_dfs = []
    c_df = hr.create_c_df_from_bank(bank_files[0])
    name = project_path+'/processed_data/'
    c_df.to_csv(name+'_c.csv', index = False)
    for file in response_files:
        f_df = hr.process_response_string_file(file, bank_files[0], get_df = True, create_c = False)
        f_name = project_path + '/processed_data/' + hfh.get_stem(file) + '_f.csv'
        f_df.to_csv(f_name, header=False, index=False)
        f_dfs.append(f_df)
    name = name+'_'+month+'_'+day+'_'+year[2:4]+'_CUMULATIVE_f.csv'
    final_f_df = hfh.pd.concat(f_dfs)
    final_f_df.to_csv(name, header=False, index = False)

def create_form_report(project_path, f_path, c_path):
    #   passing is defined as a number after the last _
    date_start = hfh.get_stem(f_path).find('_')
    date_finish = hfh.get_stem(f_path).rfind('_')
    cumulative = False
    if f_path.find("CUMULATIVE")>0:
        cumulative = True
    ret = []

    if date_start > 0 and date_finish > 0 or cumulative:
        date = '_CUMULATIVE_'
        if not cumulative:
            date = hfh.get_stem(f_path)[date_start+1:date_finish]
        ret.append(["DATE", date])
        bank_file = hfh.get_single_file(project_path, target_string='xlsx')
        passing_score = bank_file[bank_file.rfind('_')+1:bank_file.rfind('.')]
        assert int(passing_score), 'PASSING SCORE IS DEFINED AS NAME_SCORE.xlsx'
        passing_score = int(passing_score)
        f_df = hfh.get_df(f_path)
        c_df = hfh.get_df(c_path, header = 0)
        graded_df = hr.grade_examination(f_df, c_df,grading_processed=True, incorrect=0)
        n = graded_df.shape[0]
        ret.append(['N',n])
        graded_df['SCORE'] = graded_df.sum(axis = 1)
        graded_df['PASS'] = graded_df['SCORE']>=passing_score
        proportion_of_candidates_who_pass = graded_df['PASS'].mean()
        ret.append(["PASS_P", hfh.c_round(proportion_of_candidates_who_pass,as_string=True, as_percentage=True)])
        graded_df['MARGINAL'] = abs(passing_score - graded_df['SCORE']) < 2
        marginal = sum(graded_df['MARGINAL']) / graded_df.shape[0]
        ret.append(['BOARDERLINE',hfh.c_round(marginal,as_string=True, as_percentage=True)])
        average_pbis = graded_df.corr()['SCORE'].mean()
        ret.append(['PBIS', hfh.c_round(average_pbis)])
        average_score = graded_df['SCORE'].mean()
        ret.append(['AVERAGE_SCORE',hfh.c_round(average_score)])
        top_10 = graded_df['SCORE'].quantile([.9])
        top_10 = int(top_10.values[0])
        ret.append(['TOP10',top_10])
        bottom_10 = int(graded_df['SCORE'].quantile([.1]).values[0])
        ret.append(['BOTTOM10', bottom_10])
        SEM = graded_df['SCORE'].std()/pow(graded_df.shape[0],.5)
        ret.append(['SEM', hfh.c_round(SEM)])
        alpha = get_alpha(graded_df)
        ret.append(['ALPHA', hfh.c_round(alpha)])
        ret =  hfh.pd.DataFrame(ret).T
        ret.columns = ret.loc[0]
        ret = ret.set_index(ret['DATE'])
        ret = ret.iloc[1,:]
        return ret


    else:
        print(f_path + " was not configures as _f with date information")

def get_alpha(graded_df):
    N = graded_df.shape[1]
    all_correct = graded_df.min() == 1
    '''
    graded_df = graded_df.T[~all_correct]
    graded_df = graded_df.apply(hfh.pd.to_numeric, errors = 'ignore')
    graded_df = graded_df.T
    '''
    item_var = graded_df.var()
    item_var = item_var.sum()
    score_var = graded_df.sum(axis = 1)
    score_var = score_var.var()
    alpha = (N/(N-1))*(1-item_var/score_var)
    return alpha

def create_classical_stats(project_path):
    # creates a report per response string file
    f_files = hfh.get_all_files(project_path + '/processed_data',target_string='_f.csv')
    c_files = hfh.get_all_files(project_path + '/processed_data',target_string='_c.csv')
    assert len(c_files) == 1, "there should be exactly 1 _c file in processed data."
    c_file = c_files[0]
    c_df = hfh.get_df(c_file, header = 0)

    stats = []
    for file in f_files:
        f_df = hfh.get_df(file)
        graded = hr.grade_examination(f_df, c_df, correct=1, incorrect=0, only_operational=False, grading_processed=True)
        form = hfh.get_stem(file)
        _i = form.find(' ')
        name = form[:_i]
        classical_stats = hr.create_classical_stats_from_graded(graded, form_name=name)
        stats.append(classical_stats)
    cumulative_df = hfh.pd.concat(stats, axis = 1)
    create_flagged(cumulative_df, project_path)

def create_flagged(cumulative_df, project_path):
    cols = cumulative_df.columns.values
    target_cols_pbis = []
    target_cols_p = []
    for col in cols:
        if col.find('FLAG_PBIS')>-1:
            target_cols_pbis.append(col)
        if col.find('FLAG_P')>-1:
            target_cols_p.append(col)
    total_pbis_flags = cumulative_df[target_cols_pbis]
    total_p_flags = cumulative_df[target_cols_p]
    total_pbis_flags = total_pbis_flags.sum(axis = 1)
    total_p_flags = total_p_flags.sum(axis=1)
    cumulative_df['%_PBIS_FLAGS'] = total_pbis_flags/len(target_cols_pbis)
    cumulative_df['%_P_FLAGS'] = total_p_flags/len(target_cols_p)
    cumulative_df.to_csv(project_path + '/reports/' + 'CUMULATIVE_REPORT.csv')

def pull_responses_from_combined_folders(path, program_string):
    files = hfh.get_all_files(path, target_string=program_string)
    target_path = path+'/'+program_string
    hfh.create_dir(target_path)
    for file in files:
        lines = hfh.get_lines(file)
        hfh.write_lines_to_text(lines,target_path+'/'+hfh.get_stem(file)+'_d.txt')

def create_10_day_stats(project_path, pull_from_combined = False, program_string = None):
    if pull_from_combined:
        assert program_string is not None, "WHEN PULLING FROM COMBINED MUST DEFINE PROGRAM STRING"
        pull_responses_from_combined_folders(project_path,program_string)
        project_path = project_path + '/' + program_string
    establish_folders(project_path)
    create_processed(project_path)
    create_classical_stats(project_path)

