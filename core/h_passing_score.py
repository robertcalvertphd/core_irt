import core.h_raw_processor as hr
import core.h_simple_visualizations as hv
hfh = hr.hfh
from math import pow,e

# This file requires:
#   stats.csv, tif.csv, iif.csv
#   bank.xlsx of target form or key of target form
#   a key is defined as Position, AccNum


def get_p_for_item_given_theta_and_b(theta, B):
    theta = float(theta)
    B = float(B)
    y = theta - B
    top = pow(e, y)
    bottom = 1 + pow(e, theta-B)
    return top/bottom


def get_passing_from_bs_and_theta(bs, theta):
    ret = 0
    for b in bs:
        ret+= get_p_for_item_given_theta_and_b(theta,b)
    return ret/len(bs)


def get_theta_from_percent_correct(percent, path_to_tif = None, tif_df = None, return_stem = False):
    if tif_df is None:
        df = hfh.pd.read_csv(path_to_tif)
    else:
        df=tif_df
    SCORE_COL = "TRF"
    THETA_COL = "Theta"
    a = df.iloc[(df[SCORE_COL].astype(float) - percent).abs().argsort()[:1]]
    a = float(a[THETA_COL].values[0])
    if return_stem:
        return hfh.get_stem(path_to_tif), int(a * 100) / 100
    return int(a * 100) / 100


def get_p_from_form_theta(theta, tif_path = None, tif_df = None):
    if tif_df is None:
        df = hfh.pd.read_csv(tif_path)
    else:
        df=tif_df
    SCORE_COL = "TRF"
    THETA_COL = "Theta"
    a = df.iloc[(df[THETA_COL].astype(float) - theta).abs().argsort()[:1]]
    a = a[SCORE_COL].values[0]
    return a


def create_passing(project_path, form_path, passing_theta,
                   remove_version = False, verbose = True, form_df = False,
                   cross_validation = False, stats_path = False, useComplete = False,
                   create_form_info = False, create_histogram = True):
    #todo: remove_version should be a default in processing and not this far along.
    EASY_DELTA_FROM_THETA = 1.3
    HARD_DELTA_FROM_THETA = .4
    x_path = project_path + '/xCalibre/final'
    cal_path = project_path + '/calibration/final'

    if useComplete:
        x_path = project_path + '/xCalibre/initial'
        cal_path = project_path + '/calibration/initial'
    if cross_validation:
        x_path = project_path + '/cross_validation'
    if not stats_path:
        stats_path = hfh.get_all_files(x_path,target_string='Stats')
        tif_path = hfh.get_all_files(x_path,target_string='TIF.csv')
    culled_path = hfh.get_all_files(cal_path,target_string='CULLED')
    complete_path = hfh.get_all_files(project_path + '/calibration/initial', target_string='COMPLETE')


    if len(stats_path) == 1:
        stats_path = stats_path[0]
    else:
        print("issue with stats path in xCalibre folder. Requires final calibration.")
        return False
    if len(tif_path) == 1:
        tif_path = tif_path[0]
    else:
        print("issue with TIF path in xCalibre folder.")
        return False
    if not len(culled_path)*len(complete_path)==1:
        print("issue culled and complete.")
        return False
    culled_path = culled_path[0]
    complete_path = complete_path[0]
    stats = hfh.get_stats_df(stats_path, remove_na=False, remove_version=remove_version)
    if form_df is False:
        form = hfh.get_df(form_path, header = 0)
    else:
        form = form_df
    ids = form['AccNum']
    mask_form_items = stats['Item ID'].isin(form['AccNum'])
    # reporting items not included in analysis
    not_included = ids[~ids.isin(stats['Item ID'])]
    complete_df = hfh.get_df(complete_path, header = 0)
    complete_df = complete_df.sort_values(by='AccNum')
    #   todo: order will matter here... as of current the order is
    #    1) remove drifting items: this is currently done manually to X_INITITIAL_c, _f
    #    2) cull items (initial)
    #    3) cull items (final)


    culled_items = not_included[not_included.isin(complete_df['AccNum'])]
    no_information_items = not_included[~not_included.isin(complete_df['AccNum'])]
    drift_path = hfh.get_single_file(project_path + '/reports',target_string='drift')
    drift_ids = []
    if drift_path is not False:
        drift_df = hfh.get_df(drift_path, header = 0)
        drift_ids = drift_df['AccNum']
    drift_items = []
    if len(drift_ids)>0:
        drift_items = no_information_items[no_information_items.isin(drift_ids)]
        no_information_items = no_information_items[~no_information_items.isin(drift_ids)]

    not_str = ""
    if len(culled_items)>0:
        not_str = "\n" +"Low quality items:" +'\n'
        count = 0
        for i in culled_items:
            count += 1
            not_str += i
            if count % 4 == 0:
                not_str += '\n'
            else:
                not_str += ','
        not_str = not_str.strip(",")
        #
    if len(no_information_items)>0:
        not_str += '\n' + "Items without data:" +'\n'
        count = 0
        for i in no_information_items:
            count += 1
            not_str += i
            if count % 4 == 0:
                not_str += '\n'
            else:
                not_str += ','
        not_str = not_str.strip(",")

    if len(drift_items)>0:
        not_str += '\n' + "Items excluded due to detected drift:" +'\n'
        count = 0
        for i in drift_items:
            count += 1
            not_str += i
            if count % 4 == 0:
                not_str += '\n'
            else:
                not_str += ','
        not_str = not_str.strip(",")

    ret = 0
    #stats = stats.set_index(stats['Item ID'])
    #form_items = form_items.set_index(form_items['AccNum'])

    form_items = stats[mask_form_items]
    form_items = form_items.sort_values(by = 'b', ascending=False)
    if create_form_info:
        print("creating form info for " + form_path)
        form_items.to_csv(hfh.get_stem(form_path) + '_ITEM_INFO.csv')

    _ten_most_difficult = form_items.head(10)
    bs = stats[mask_form_items]['b']
    omitted = 0
    high_B = 0
    low_B = 0

    str_hi_b = []

    counter = -1
    for b in bs:
        counter += 1
        if b == 'Removed':
            omitted += 1
        else:
            x = get_p_for_item_given_theta_and_b(passing_theta, float(b))
            if float(b) > passing_theta + HARD_DELTA_FROM_THETA:
                high_B += 1

            if float(b) < passing_theta - EASY_DELTA_FROM_THETA:
                low_B += 1
            ret += x
    if len(bs)>0:
        p = ret * 100 / len(bs)
    else:
        p = 1.99
        print(form_path + " seems to have an issue. p arbitrarily set to 1.99")
    if verbose:
        n = len(bs)
        name = hfh.get_stem(form_path)
        passing_reference_as_p = get_p_from_form_theta(passing_theta, tif_path=tif_path)
        p = round(p, 2)
        first_line = "######" + name + " CUT = " + str(int(p*n/100)) + " of " + str(n) + " = "+ str(p) + "% ########"
        print(first_line)
        print("Bank passing p set at " + str(passing_reference_as_p))
        print("Bank theta set at " + str(passing_theta))
        print("n = " + str(len(bs)))
        print("Difficult items\t" + str(high_B) + " Bs are > " + str(HARD_DELTA_FROM_THETA + passing_theta))
        print("Easy items     \t" + str(low_B)  + " Bs are < " + str(round(passing_theta - EASY_DELTA_FROM_THETA, 2)))
        block = ""
        for n in range(len(first_line)):
            block += '#'
        if create_histogram:
            print(block)
            print("Difficulty Histogram\n")
            print("Larger number are more difficult.\n")
            print("Compare to bank theta. \nAn individual with ability = b will get it right 50% of the time.")
            hv.create_histogram(bs, columns=8, unit=1)
        if len(not_included) > 0:
            print("___________________\n MISSING DATA INFORMATION \n___________________\n" + not_str)
        #print(_ten_most_difficult['Item ID'])
        last_line = ""

        if len(str_hi_b) > 0:
            not_str += '\n' + "Difficult items:" + '\n'
            count = 0
            for i in str_hi_b:
                count += 1
                not_str += i
                if count % 4 == 0:
                    not_str += '\n'
                else:
                    not_str += ','
            not_str = not_str.strip(",")
        for i in range(len(first_line)):
            last_line += '#'
        print(last_line)
    return p

def get_passing_form_info(project, form):
    culled = hfh.get_single_file(project+'/calibration/final',target_string='CULLED', as_df=True, header = 0).sort_values('AccNum')
    complete = hfh.get_single_file(project + '/calibration/initial', target_string='COMPLETE', as_df = True, header = 0).sort_values('AccNum')
    form_df = hfh.get_df(form,header=0)
    form_culled = culled[culled['AccNum'].isin(form_df['AccNum'])].sort_values('AccNum')
    form_complete = complete[complete['AccNum'].isin(form_df['AccNum'])].sort_values('AccNum')

    print("hello")








def get_pass_rate(cut,f_df, c_df, round_down = True, grading_processed = True, graded = None):
    if graded is None:
        if grading_processed:
            graded = hr.grade_examination(f_df, c_df, correct=1, incorrect=0,only_operational=True, grading_processed=grading_processed)
        else:
            graded = f_df
            graded = graded.replace('1', float(1.0))
            graded = graded.replace('2', float(0.0))

    items = f_df.shape[1]-1
    required_to_pass = int(cut*items)
    if not round_down:
        required_to_pass += 1
    graded['SCORE'] = graded.sum(axis = 1)
    graded['PASS'] = 0
    graded.loc[graded['SCORE']>required_to_pass,'PASS']=1
    max_score = graded['SCORE'].max()
    high = graded.sort_values(by='SCORE')
    return graded['PASS'].sum()/graded.shape[0]