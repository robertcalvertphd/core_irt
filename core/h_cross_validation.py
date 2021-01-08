import core.h_passing_score as hp
hfh = hp.hfh
ph = hfh.pd


def create_cross_validation_data(project, train_proportion = .8):
    calibration_folder = project + "/" + hfh.E.INITIAL_CALIBRATION_P
    cross_folder = project + '/' + hfh.E.CROSS_VALIDATION_P
    name = project[project.rfind('/')+1:]

    f_df = hfh.get_single_file(calibration_folder, target_string='_f', as_df=True)
    c_df = hfh.get_single_file(calibration_folder, target_string='_c', as_df=True)

    sample = f_df.sample(frac=1-train_proportion)
    calibrate = f_df[~f_df.index.isin(sample.index)]

    sample.to_csv(cross_folder+'/'+ name + hfh.E.VALIDATION_GROUP_F, index=False, header=False)
    calibrate.to_csv(cross_folder+'/'+ name + hfh.E.TRAINING_GROUP_F, index = False, header = False)
    c_df.to_csv(cross_folder + '/' + name + '_c.csv', index = False, header = False)

#step 2
#   RUN xCalibre

#step 3
def assess_form(project, form, theta):
    passing_from_CALIBRATE_GROUP = hp.create_passing(project, form, theta, cross_validation=True)
    passing_from_POPULATION_GROUP = hp.create_passing(project, form, theta)

    difference = passing_from_CALIBRATE_GROUP-passing_from_POPULATION_GROUP
    data_name = hfh.get_stem(form)[:-2]+'_f'
    control_name = hfh.get_stem(form)[:-2]+'_c'
    f_df = hfh.get_single_file(project+'/processed_data', target_string= data_name, as_df=True)
    c_df = hfh.get_single_file(project+'/processed_data', target_string= control_name, as_df = True)

    pass_rate_cal = hp.get_pass_rate(c_df=c_df, f_df=f_df, cut = passing_from_CALIBRATE_GROUP)
    pass_rate_pop = hp.get_pass_rate(c_df=c_df, f_df=f_df, cut = passing_from_POPULATION_GROUP)
    sample_f_df = hfh.get_single_file(project+'/cross_validation', target_string= 'SAMPLE', as_df = True)
    sample_ids = sample_f_df.iloc[:,0]

    if abs(pass_rate_cal - pass_rate_pop) > 0:
        print("different")


    #   difference in cut_score from calibration to complete
    #   actual difference in pass rate from two on sample group

# step 4
def compare_big_group_and_small_group(project):
    big_name = 'CALIBRATE_GROUP Stats'
    little_name = 'SAMPLE_GROUP Stats'
    big_stats_path = hfh.get_single_file(project + '/cross_validation', target_string=big_name)
    lil_stats_path = hfh.get_single_file(project + '/cross_validation', target_string=little_name)
    big_df = hfh.get_stats_df(big_stats_path)
    lil_df = hfh.get_stats_df(lil_stats_path)
    big_df = big_df[['Item ID', 'P', 'b', 'T-Rpbis']]
    lil_df = lil_df[['Item ID', 'P', 'b', 'T-Rpbis']]
    matching = hfh.pd.merge(left=lil_df, right=big_df, on='Item ID', suffixes=['_LIL', '_BIG'])
    test_cor = matching.corr()

    print("hello")

