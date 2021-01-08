import core.h_raw_processor as hr
import datetime
import core.h_proexam_upload as hp
hfh = hr.hfh


class Handler10:
    debug = True

    def __init__(self, ignore = [], process_all_programs = True):
        self.classical_path = 'G:\LICENSING EXAMINATION\Shared\_CLASSICAL_STATS'
        persons = hfh.get_all_folders_in_folder(self.classical_path)
        self.log_entries = []
        self.ignore = ignore
        if process_all_programs:
            for person in persons:
                programs = hfh.get_all_folders_in_folder(person)
                for program in programs:
                    program_name = program + '.trash'
                    program_name = hfh.get_stem(program_name)
                    if program_name not in self.ignore:
                        forms = hfh.get_all_folders_in_folder(program)
                        for form in forms:
                            if self.confirm_setup(form):
                                self.process_form(form)

    def process_form(self, form):
        if self.confirm_setup(form):
            print("processing:" + form)
            self.log_entries.clear()
            self.confirm_setup(form)
            self.create_processed(form)
            self.create_classical_stats(form)
            self.create_ten_day_report(form)
            self.record_log(form)

    def reset_data(self, form):
        raw_backup = hfh.get_all_file_names_in_folder(form+'/raw_data_backup')
        for file in raw_backup:
            name = hfh.get_stem(file) +"."+ hfh.get_extension(file)
            hfh.move_file(file, form+'/unprocessed_data/' + name)

    def confirm_setup(self,form):
        valid_setup = True
        #   checks that there are unprocessed files
        self.establish_folders(form)
        #   check for bank_file
        bank = hfh.get_single_file(form, target_string='.xlsx')
        if bank is False:
            valid_setup = False
            self.log(form + ' bank is not present.')
        #   establish form cut score is suffix of bank
        else:
            cut = self.get_cut_score_from_bank_name(bank)
            if cut is False:
                valid_setup = False
                self.log(bank + " does not follow canonical form NAME_CUT.xlsx")
        unprocessed_files = hfh.get_all_file_names_in_folder(form+'/unprocessed_data')

        if len(unprocessed_files) == 0:
            valid_setup = False
            self.log(form + " has no unprocessed files and was consequently ignored.")
        self.log("valid setup:" + str(valid_setup))
        return valid_setup


    def get_cut_score_from_bank_name(self,bank_path):
        if bank_path is not False:
            bank = hfh.get_stem(bank_path)
            i_ = bank.rfind('_')
            if i_ > -1:
                cut = bank[i_+1:]
                if cut.isdigit():
                    self.log("CUT SET AT" + cut)
                    return int(cut)
            else:
                print("no cut score in bank named: " + bank_path)
        return False

    def create_processed(self, form):
        project_path = form
        d = datetime.datetime.today()
        day = str(d.day)
        month = str(d.month)
        year = str(d.year)
        #   move everything that is not in a folder to raw_data
        #   validate the contents of raw data
        #   process raw data to _c and _f
        response_files = hfh.get_all_file_names_in_folder(project_path + '/unprocessed_data')
        if len(response_files)>0:
            bank_files = hfh.get_all_files(project_path, extension='xlsx')
            assert len(bank_files) == 1, "THERE MUST BE EXACTLY ONE BANK FILE IN " + project_path

            f_dfs = []
            #add already processed to f_dfs
            f_files = hfh.get_all_files(project_path + '/processed_data')
            for file in f_files:
                if file.find('_c.csv')==-1 and file.find('CUMULATIVE')==-1 and file.find('_f.csv')>0:
                    f_df = hfh.get_df(file,header = None, index_col=0)
                    f_dfs.append(f_df)
            c_df = hr.create_c_df_from_bank(bank_files[0])
            name = project_path + '/processed_data/'
            c_df.to_csv(name + '_c.csv', index=False)
            for file in response_files:
                if hr.is_valid_name(file):
                    f_df = hr.process_response_string_file(file, bank_files[0], get_df=True, create_c=False)
                    f_name = project_path + '/processed_data/' + hfh.get_stem(file) + '_f.csv'
                    f_df.to_csv(f_name, header=None)
                    f_dfs.append(f_df)
                    file_name = hfh.get_stem(file)
                    extension = hfh.get_extension(file)
                    # todo let it move once finished debugging.
                    #hfh.move_file(file, form+'/raw_data_backup/'+file_name+'.'+extension)
            name = name + '_' + month + '_' + day + '_' + year[2:4] + '_CUMULATIVE_f.csv'
            if len(f_dfs) == 0:
                self.log("no processed data found")
            else:
                final_f_df = hfh.pd.concat(f_dfs)
                #todo: index should always be person ID... if it is not something is wrong. Fix on back end.
                final_f_df.to_csv(name, header=False, index=True)
        else:
            self.log("no unprocessed data found.")


    def establish_folders(self,form):
        name = "unnamed"
        project_path = form
        unprocessed_path = project_path + '/unprocessed_data'
        raw_data_path = project_path + '/raw_data_backup'
        report_path = project_path + '/reports'
        processed_path = project_path + '/processed_data'
        log_path = project_path + '/logs'
        if not hfh.os.path.isdir(unprocessed_path):
            hfh.create_dir(unprocessed_path)
            files = hfh.get_all_files(form)
            for file in files:
                ext = hfh.get_extension(file)
                if ext == 'xlsx':
                    pass
                else:
                    name = hfh.get_stem(file) + '.' + hfh.get_extension(file)
                    hfh.move_file(file,unprocessed_path+'/'+name)
        hfh.create_dir(report_path)
        hfh.create_dir(processed_path)
        hfh.create_dir(raw_data_path)
        hfh.create_dir(log_path)
        hfh.create_dir(report_path+'/ugly')

    def log(self,  message):
        self.log_entries.append(message + '\n')

    def record_log(self, form):
        log_path = form+'/logs/'
        t = datetime.datetime.today()
        string_date = str(t.day) + str(t.month) + str(t.year)[2:]
        log_name = "LOG_" + string_date + '.txt'

        hfh.write_lines_to_text(self.log_entries,log_path + log_name)

    def create_form_report(self, form, f_path, c_path):
        #   passing is defined as a number after the last _
        project_path = form
        date_start = hfh.get_stem(f_path).find('_')
        date_finish = hfh.get_stem(f_path).rfind('_')
        cumulative = False
        if f_path.find("CUMULATIVE") > 0:
            cumulative = True
        ret = []

        if date_start > 0 and date_finish > 0 or cumulative:
            date = '_CUMULATIVE_'
            if not cumulative:
                date = hfh.get_stem(f_path)[date_start + 1:date_finish]
            ret.append(["DATE", date])
            bank_file = hfh.get_single_file(project_path, target_string='xlsx')
            passing_score = bank_file[bank_file.rfind('_') + 1:bank_file.rfind('.')]
            assert int(passing_score), 'PASSING SCORE IS DEFINED AS NAME_SCORE.xlsx'
            passing_score = int(passing_score)
            f_df = hfh.get_df(f_path)
            c_df = hfh.get_df(c_path, header=0)
            graded_df = hr.grade_examination(f_df, c_df, grading_processed=True, incorrect=0, only_operational=True)
            n = graded_df.shape[0]
            ret.append(['N', n])
            graded_df['SCORE'] = graded_df.sum(axis=1)
            graded_df['PASS'] = graded_df['SCORE'] >= passing_score
            proportion_of_candidates_who_pass = graded_df['PASS'].mean()
            ret.append(["PASS_P", hfh.c_round(proportion_of_candidates_who_pass, as_string=True, as_percentage=True)])
            graded_df['MARGINAL'] = abs(passing_score - graded_df['SCORE']) < 2
            marginal = sum(graded_df['MARGINAL']) / graded_df.shape[0]
            if marginal>0:
                marginal = hfh.c_round(marginal, as_string=True, as_percentage=True)
            else:
                marginal = 0
            ret.append(['BOARDERLINE', marginal])
            average_pbis = graded_df.corr()['SCORE'].mean()
            ret.append(['PBIS', hfh.c_round(average_pbis)])
            average_score = graded_df['SCORE'].mean()
            if average_score >0:
                average_score = hfh.c_round(average_score)
            else:
                average_score = 0
            ret.append(['AVERAGE_SCORE', average_score])
            top_10 = graded_df['SCORE'].quantile([.9])
            top_10 = int(top_10.values[0])
            ret.append(['TOP10', top_10])
            bottom_10 = int(graded_df['SCORE'].quantile([.1]).values[0])
            ret.append(['BOTTOM10', bottom_10])
            SEM = graded_df['SCORE'].std() / pow(graded_df.shape[0], .5)
            ret.append(['SEM', hfh.c_round(SEM)])
            alpha = self.get_alpha(graded_df)
            ret.append(['ALPHA', hfh.c_round(alpha)])
            min = graded_df['SCORE'].min()
            ret.append(['MIN_S',min])
            max = graded_df['SCORE'].max()
            ret.append(['MAX_S', max])
            ret.append(['STD_S', hfh.c_round(graded_df['SCORE'].std(),2)])
            ret = hfh.pd.DataFrame(ret).T
            ret.columns = ret.loc[0]
            ret = ret.set_index(ret['DATE'])
            return ret

        else:
            if f_path.find('_c.csv')==-1:
                print(f_path + " was not configured as _f with date information")

    def get_alpha(self,graded_df):
        N = graded_df.shape[1]
        all_correct = graded_df.min() == 1
        '''
        graded_df = graded_df.T[~all_correct]
        graded_df = graded_df.apply(hfh.pd.to_numeric, errors = 'ignore')
        graded_df = graded_df.T
        '''
        item_var = graded_df.var()
        item_var = item_var.sum()
        score_var = graded_df.sum(axis=1)
        score_var = score_var.var()
        alpha = (N / (N - 1)) * (1 - item_var / score_var)
        return alpha

    def create_classical_stats(self, form):
        project_path = form
        # creates a report per response string file
        f_files = hfh.get_all_files(project_path + '/processed_data', target_string='_f.csv')
        c_files = hfh.get_all_files(project_path + '/processed_data', target_string='_c.csv')
        assert len(c_files) == 1, "there should be exactly 1 _c file in processed data."
        c_file = c_files[0]
        c_df = hfh.get_df(c_file, header=0)

        stats = []
        # creating classical aggregate report
        for file in f_files:
            if file.find("CUMULATIVE")==-1:
                f_df = hfh.get_df(file)

                graded = hr.grade_examination(f_df, c_df, correct=1, incorrect=0, only_operational=False)
                form = hfh.get_stem(file)
                _i = form.find(' ')
                name = form[:_i]
                classical_stats = hr.create_classical_stats_from_graded(graded, form_name=name)
                stats.append(classical_stats)
                cumulative_df = hfh.pd.concat(stats, axis=1)
        self.create_flagged(cumulative_df, project_path)

        # creating item level classical report
        item_level_classical_stats = []
        cumulative_classical_stats = None

        for file in f_files:
            form = hfh.get_stem(file)
            _i = form.find(' ')
            df = hp.create_upload_from_processed(c_file, file)
            new_cols = []
            for col in df.columns:
                new_cols.append(col+'_'+name)
            df.columns = new_cols
            item_level_classical_stats.append(df)
            if file.find("CUMULATIVE")>-1:
                df.to_csv(project_path + '/reports/' + name + '_CUMULATIVE_ITEM_STATS.csv')
        if len(item_level_classical_stats)>0:
            item_level = hfh.pd.concat(item_level_classical_stats, axis=1)
            item_level.to_csv(project_path+'/reports/ugly/'+name+'COMPLETE_ITEM_STATS_.csv')

    def create_flagged(self, cumulative_df, project_path):
        #todo: add name to all files so you can't be confused.
        cols = cumulative_df.columns.values
        target_cols_pbis = []
        target_cols_p = []
        for col in cols:
            if col.find('FLAG_PBIS') > -1:
                target_cols_pbis.append(col)
            if col.find('FLAG_P') > -1:
                target_cols_p.append(col)
        total_pbis_flags = cumulative_df[target_cols_pbis]
        total_p_flags = cumulative_df[target_cols_p]
        total_pbis_flags = total_pbis_flags.sum(axis=1)
        total_p_flags = total_p_flags.sum(axis=1)
        cumulative_df['%_PBIS_FLAGS'] = total_pbis_flags / len(target_cols_pbis)
        cumulative_df['%_P_FLAGS'] = total_p_flags / len(target_cols_p)
        #cumulative_df['P_~K'] = 1-cumulative_df['P']
        #cumulative_df['P_MAX'] = cumulative_df[['a_P', 'b_P', 'c_P','d_P']].max()
        #cumulative_df['P_DISTRACTOR_FLAG'] = cumulative_df['P_MAX'] != cumulative_df['P']
        short_report = cumulative_df[['%_PBIS_FLAGS','%_P_FLAGS']]
        short_report = short_report[(short_report['%_PBIS_FLAGS']>0) | (short_report['%_P_FLAGS']>.33) ]
        short_report.to_csv(project_path + '/reports/SHORT_ITEM_REPORT.csv')
        cumulative_df.to_csv(project_path + '/reports/ugly/CUMULATIVE_REPORT.csv')

    def create_ten_day_report(self, form):
        f_files = hfh.get_all_file_names_in_folder(form+'/processed_data')
        _c = hfh.get_single_file(form+'/processed_data', target_string='_c.csv')
        ret = []
        for file in f_files:
            rep10 = self.create_form_report(form,file,_c)
            if rep10 is not None:
                rep10 = rep10.drop('DATE')
                ret.append(rep10)
        if len(ret)>0:
            name = hfh.get_stem(f_files[0])
            i = name.find('_')
            name = name[:i]
            path = form+ "/reports/"+name+"_10DAY_STATS.csv"

            ret = hfh.pd.concat(ret, axis=0)
            ret = ret.T
            ret.to_csv(path, header = False, index = True)
            comments = [""]
            comments.append("Date: the information between last _ and end of data file name. ")
            comments.append("N: the sample size of the response string file identified in Date.")
            comments.append("PASS_P: the percentage of candidates who met or exceeded the cut score.")
            comments.append("BORDERLINE: the percentage of candidates who were within 1 point of the cut score (both directions).")
            comments.append("PBIS: the classical point biserial correlation between correct endorsement of this item and total score.")
            comments.append("AVERAGE_SCORE: the mean of scores for candidates from the response string file identified in Date.")
            comments.append("TOP10: the minimum number of items a candidate needs to answer correctly to be in the top 10 percent of candidates.")
            comments.append("BOTTOM10: the maximum number of items a candidate needs to answer correctly to be in the bottom 10 percent of candidates.")
            comments.append("SEM: standard error of mean = sample standard divided by the root of n")
            comments.append("ALPHA: Cronbach's alpha is a measure of reliability. The measure is sensitive to sample size and number of items.\n A value less than 0.7 is concerning. A value less than 0.5 is very concerning.")
            comments.append("MIN_S: minimum score of form for Date")
            comments.append("MAX_S: maximum score of form for Date")
            comments.append("STD_S: standard deviation of score on form for Date")

            hfh.add_lines_to_csv(path, comments)


def reset_folders_CAUTION_FOR_DEBUGGING_ONLY(classical_path,set_txt=False, include = None):
    for person in include:
        programs = hfh.get_all_folders_in_folder(classical_path + '/' + person)
        for program in programs:
            forms = hfh.get_all_folders_in_folder(program)
            for form in forms:
                raw_data = hfh.get_all_files(form+'/raw_data_backup')
                processed_data = hfh.get_all_file_names_in_folder(form+'/processed_data')
                unprocessed_data = hfh.get_all_file_names_in_folder(form + '/unprocessed_data')
                reports_data = hfh.get_all_file_names_in_folder(form + '/reports')
                logs = hfh.get_all_file_names_in_folder(form + '/logs')
                for file in raw_data:
                    name = hfh.get_stem(file) + '.'+hfh.get_extension(file)
                    if debug is False:
                        hfh.move_file(file,form+'/unprocessed_data/'+name)
                '''
                for file in unprocessed_data:
                    if set_txt:
                        hfh.os.rename(file,file+'.txt')
                    else:
                        hfh.os.remove(file)
                '''
                for file in processed_data:
                    hfh.os.remove(file)
                '''
                for file in reports_data:
                    hfh.os.remove(file)
                for file in logs:
                    hfh.os.remove(file)
                '''
def fix_extension(folder_path, remove, replace = "", new_extension = None):
    files = hfh.get_all_file_names_in_folder(folder_path)
    for file in files:
        extension = hfh.get_extension(file)
        parent_folder = hfh.get_parent_folder(file)
        if new_extension is not None:
            extension = new_extension
        name = hfh.get_stem(file)
        name = str(name).replace(remove,replace)
        name += "."+extension
        new_file_path = parent_folder + '/' + name
        hfh.move_file(file, new_file_path)

#fix_extension(r'G:\LICENSING EXAMINATION\Shared\_CLASSICAL_STATS\Amy\LLE\LLE20AUG\unprocessed_data','csv',"",new_extension='csv')
#reset_folders_CAUTION_FOR_DEBUGGING_ONLY(r'G:\LICENSING EXAMINATION\Shared\_CLASSICAL_STATS', include=['_Robert'])
#h = Handler10(ignore=['CRE','CRP'])

'''
def create_10_day_stats(project_path, pull_from_combined=False, program_string=None):
    if pull_from_combined:
        assert program_string is not None, "WHEN PULLING FROM COMBINED MUST DEFINE PROGRAM STRING"
        pull_responses_from_combined_folders(project_path, program_string)
        project_path = project_path + '/' + program_string
    establish_folders(project_path)
    create_processed(project_path)
    create_classical_stats(project_path)
'''
'''
def pull_responses_from_combined_folders(path, program_string):
    files = hfh.get_all_files(path, target_string=program_string)
    target_path = path + '/' + program_string
    hfh.create_dir(target_path)
    for file in files:
        lines = hfh.get_lines(file)
        hfh.write_lines_to_text(lines, target_path + '/' + hfh.get_stem(file) + '_d.txt')
'''