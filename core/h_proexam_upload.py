import core.h_file_handling as hfh
import core.h_raw_processor as hr

def create_upload_from_processed(c_file, f_file, path = None, c_has_header = True, to_csv = False):
    #todo: decide if _c files have headers or not...
    #todo: perhaps a different _x to indicate header or not... the only time I don't want a header is xCalibre...

    if c_has_header:
        c_df = hfh.get_df(c_file, header = 0)
    else:
        c_df = hfh.get_df(c_file)

    f_df = hfh.get_df(f_file, index_col = 0)
    stats_df = hfh.pd.DataFrame([])
    stats_df['AccNum'] = c_df.iloc[:,0]
    graded_df = hr.grade_examination(f_df, c_df, grading_processed=True, correct=1, incorrect=0)

    score = graded_df.sum(axis = 1)

    pbis = graded_df[graded_df.columns[0]].corr(score)

    A = get_option_df(f_df,'A')
    B = get_option_df(f_df, 'B')
    C = get_option_df(f_df, 'C')
    D = get_option_df(f_df, 'D')

    options = ['A','B','C','D']
    dfs = [A,B,C,D]
    counter = -1

    N = ~f_df.isna()
    N = N.sum()
    N = N.reset_index(drop = True)

    for option in options:

        counter +=1
        a_ret = []
        b_ret = []
        c_ret = []

        df = dfs[counter]


        for column in A.columns:
            mask = df[column]==1
            mean_score = graded_df[mask].mean().mean()
            c_ret.append(mean_score)
            pbis = df[column].corr(score)
            endorse_p = df[column].sum()/df.shape[0]
            a_ret.append(pbis)
            b_ret.append(endorse_p)
        stats_df[option + '_r'] = hfh.pd.Series(a_ret, index=stats_df.index)
        stats_df[option + '_p'] = hfh.pd.Series(b_ret, index=stats_df.index)
        stats_df[option + '_m'] = hfh.pd.Series(c_ret, index=stats_df.index)

    k_ret = []
    for i in range(graded_df.shape[1]):
        pbis = graded_df[graded_df.columns[i]].corr(score)
        k_ret.append(pbis)
    stats_df['K_r'] = hfh.pd.Series(k_ret, index=stats_df.index)
    stats_df['KEY'] = c_df['Key']
    stats_df['N'] = N
    p = graded_df.mean(axis = 0)
    stats_df = stats_df.set_index('AccNum', drop=True)
    stats_df['P'] = p
    if path is None:
        name = hfh.get_stem(f_file)[:-2]+'_P.csv'
    else:
        name = path +'/'+ hfh.get_stem(f_file)[:-2]+'_P.csv'
    stats_df = stats_df[['KEY','K_r','P','A_p','A_r','A_m','B_p','B_r','B_m','C_p','C_r','C_m','D_p','D_r','D_m','N']]
    if to_csv:
        stats_df.to_csv(name)
    return stats_df

def get_option_df(f_df,option):
    options = ['A','B','C','D']
    options.remove(option)

    ret = f_df.replace(option, 1)
    for wrong_option in options:
        ret = ret.replace(wrong_option,0)
    ret = ret.replace(' ', hfh.nan)
    ret = ret.fillna(0)
    if 0 in ret.columns.values:
        ret = ret.set_index(0, drop = True)
    ret = ret.apply(hfh.pd.to_numeric, errors = 'coerce')
    return ret



