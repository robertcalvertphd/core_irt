#   this is intended to setup all of the data needed for 10_day stats nicely
import core.h_raw_processor as hr
from numpy import nan
import zipfile
import datetime

hfh = hr.hfh
E = hfh.E
os = hfh.os


class Form:
    # standard use of form in OPES
    def __init__(self, path, name = "_", debug = True):
        # check if initialized
        self.name = name
        self.debug = debug
        self.path = path
        self.processed_path = self.path + '/' + E.PROCESSED_DATA_P
        self.passing_score = None
        self.bank_path = None
        self.validate_folders()
        self.establish_bank_file()
        self.move_homeless_files()
        self.process_data()
        self.form_10s = self.create_Form_10s()
        self.create_10_day_report()
        _f = hfh.get_df(self.processed_path + '/_f.csv', header=None)
        _c = hfh.get_df(self.processed_path + '/_c.csv', header=0)
        final_form = Form10(name,_c, _f, self.passing_score,process_item_stats=True)
        path = self.path + '/' + E.REPORTS_P+ '/' + self.name + "_item_stats.csv"
        hfh.write_lines_to_text(lines = final_form.item_lines, file_path= path)
        # ^^ may add to this later but for now just makes 10 day stats

    def create_Form_10s(self):
        if self.debug:
            print("creating Form10s")
        # go get all the processed data dumps
        f_files = hfh.get_all_file_names_in_folder(self.processed_path, target_string='_f')
        c_df = hfh.get_df(self.processed_path+'/_c.csv', header=0)
        Form_10s = []
        for file in f_files:
            f_df = hfh.get_df(file)
            name = hfh.get_stem(file)
            _i = name.find('_')
            if _i > -1 and not name == '_f' :
                name = name[_i+1:-2]
            f = Form10(name, c_df, f_df, self.passing_score)
            Form_10s.append(f)
            '''
            if name == '_f':
                form_f = Form10(self.name, c_df, f_df, self.passing_score, process_item_stats=True)
                lines = form_f.item_lines
                hfh.write_lines_to_text(lines, self.path + '/' + E.REPORTS_P + '/' + self.name + "iteman_stats.csv")
            '''
        return Form_10s


    def establish_bank_file(self):
        if self.debug:
            print("establishing bank files")
        bank = hfh.get_all_file_names_in_folder(self.path,extension='xlsx')
        for file in bank:
            if file.find('~')>-1:
                bank.remove(file)
                print(file, " ignored.")
        assert len(bank) == 1, "There must be exactly one bank file in " + self.path
        bank = bank[0]
        bank_name = hfh.get_stem(bank)
        passing_i = bank_name.rfind('_')
        assert passing_i > -1, "Bank file must be of form NAME_SCORE in " + self.path
        self.passing_score = int(bank_name[passing_i+1:])
        self.bank_path = bank
        c_df = hr.create_c_df_from_bank(bank)
        c_df.to_csv(self.path + '/processed_data/'+'_c.csv', index=False)

    def validate_folders(self):
        # create folders
        if self.debug:
            print("validating folders")
        hfh.create_dir(self.path + '/unprocessed_data')
        hfh.create_dir(self.path + '/processed_data')
        hfh.create_dir(self.path + '/data_backup')
        hfh.create_dir(self.path + '/reports')
        hfh.create_dir(self.path + '/logs')

    def move_homeless_files(self):
        if self.debug:
            print("moving homeless files")
        txt_files = hfh.get_all_file_names_in_folder(self.path, extension='txt')
        csv_files = hfh.get_all_file_names_in_folder(self.path, extension='csv')

        for file in txt_files:
            name = hfh.get_name(file)
            destination = self.path + '/unprocessed_data/' + name
            hfh.move_file(file, destination)

        for file in csv_files:
            name = hfh.get_name(file)
            destination = self.path + '/unprocessed_data/' + name
            hfh.move_file(file, destination)

    def process_data(self):
        unprocessed_files = hfh.get_all_file_names_in_folder(self.path + '/unprocessed_data')
        data_backup = self.path+'/data_backup/'
        dfs = []
        for file in unprocessed_files:
            df = hr.process_response_string_file(file,destination_path=self.path+'/processed_data'
                                            ,write_csv=True, get_df=True)
            dfs.append(df)
            name = hfh.get_name(file)
            hfh.move_file(file,data_backup+name)
        if len(unprocessed_files)> 1:
            f = hfh.pd.concat(dfs)
            '''
            now = datetime.datetime.now()
            day = str(now.day)
            month = now.strftime("%b")
            '''
            name = "_f.csv"
            f.to_csv(self.path+'/'+E.PROCESSED_DATA_P+'/' + name, header = False)

    def create_10_day_report(self):
        #   this should not be triggered if there is no new data.
        #   this should not be triggered if there is no processed data.
        assert type(self.form_10s) == list, "form_10s was incorrectly initialized"
        assert len(self.form_10s)>0, "Form does not have instances of class form_10"
        form10_reports = []
        names = []
        for f in self.form_10s:
            if f.ignored is False:
                form10_reports.append(f.ten_day_entry)
                names.append(f.name)
        test = hfh.pd.concat(form10_reports, axis = 1)
        test.columns = names
        name = f.name
        ten_day_path = self.path + '/reports/ten_day_stats_'+ name + '.csv'
        test.to_csv(ten_day_path)

        comments = [""]
        comments.append("N: the sample size of the response string file identified at the top of the column(Date).")
        comments.append("PASS_RATE: the percentage of candidates who met or exceeded the cut score.")
        comments.append(
            "ALPHA: Cronbach's alpha is a measure of reliability. The measure is sensitive to sample size and number of items.\n A value less than 0.7 is concerning. A value less than 0.5 is very concerning.")
        comments.append(
            "MEAN_SCORE: the mean average of scores for candidates from the response string file identified at the top of the column.")
        comments.append(
            "MEAN_PBIS: the mean average classical point biserial correlation between correct endorsement of items and total score.")
        comments.append("SEM: standard error of mean = sample standard deviation divided by the root of n")
        comments.append("STD: standard deviation of score on form for Date")
        comments.append("MIN_S: minimum score of form for Date")
        comments.append("MAX_S: maximum score of form for Date")

        comments.append(
            "MARGINAL: the percentage of candidates who were within 1 point of the cut score (below at and above).")

        hfh.add_lines_to_csv(ten_day_path, comments)


class Form10:
    # a chunk of data for a form e.g. LEX20SEP 090120 thru 091520.txt
    def __init__(self, name, c_df, f_df, passing_score, process_item_stats = False, ignore_small_sample = False):
        self.log = []
        self.name = name
        self.c_df = c_df
        self.f_df = hr.get_strict_format_f_df(c_df, f_df)
        self.ignored = False

        if f_df.shape[0]<10 and ignore_small_sample:
            print(name + "had less than 10 candidates and is therefore ignored.")
            self.ignored = True
        else:
            self.operational_items = None
            self.graded_df = self.get_graded_for_10_day(operational=False)
            self.ten_day_entry = self.create_10_day_entry(passing_score)
            self.ten_day_items = self.create_items_stats()
            self.item_lines = None
            if process_item_stats:
                self.item_lines = self.process_item_stats(returns_lines=True)

    def get_graded_for_10_day(self, operational):
        f_df = self.f_df
        c_df = self.c_df
        graded = hr.strict_grade(c_df, f_df)
        operational_score = graded.sum(axis = 1)
        self.operational_items = graded.shape[1]
        graded = hr.strict_grade(c_df, f_df, operational=False)
        graded['SCORE'] = operational_score
        return graded


    def create_10_day_entry(self, passing_score):
        pbis = get_pbis(self.graded_df)
        #pbis = self.graded_df.corr()['SCORE']
        #pbis = pbis.drop('SCORE')
        std = hfh.c_round(self.graded_df['SCORE'].std())
        SEM = hfh.c_round(std / pow(self.graded_df.shape[0], .5))
        average_score = hfh.c_round(self.graded_df['SCORE'].mean())
        average_pbis = hfh.c_round(pbis.mean())
        self.graded_df['PASS'] = self.graded_df['SCORE'] >= passing_score
        proportion_of_candidates_who_pass = self.graded_df['PASS'].mean()
        proportion_of_candidates_who_pass = hfh.c_round(
            proportion_of_candidates_who_pass,
            as_string=True,
            as_percentage=True)
        alpha = hfh.c_round(get_alpha(self.graded_df))
        self.graded_df = self.graded_df.drop(columns=['PASS'])
        _min = self.graded_df['SCORE'].min()
        _max = self.graded_df['SCORE'].max()
        _n = self.graded_df.shape[0]
        self.graded_df['MARGINAL'] = abs(passing_score - self.graded_df['SCORE']) < 2
        marginal = sum(self.graded_df['MARGINAL']) / self.graded_df.shape[0]
        if marginal > 0:
            marginal = hfh.c_round(marginal, as_string=True, as_percentage=True)
        else:
            marginal = 0
        index = ["N", "PASS_RATE","CRONBACH_ALPHA","MEAN_SCORE","MEAN_PBIS","SEM","STD","MIN","MAX","MARGINAL"]
        ret = hfh.pd.Series([_n, proportion_of_candidates_who_pass,alpha,average_score,
                             average_pbis, SEM, std, _min, _max, marginal],index)
        return ret

    def create_items_stats(self):
        f_df = self.f_df.T
        #assumes only ABCD as possible answers breaks otherwise
        df_list = []
        for search_string in ['A', 'B', 'C','D']:
            # use above method but rename the series instead of setting to
            # a columns. The append to a list.
            df_list.append(f_df.apply(lambda x: x.str.contains(search_string))
                           .sum(axis=1)
                           .astype(int)
                           .rename(search_string)
                           )

        # concatenate the list of series into a DataFrame with the original df
        f_df = hfh.pd.concat([f_df] + df_list, axis=1)

        for item in ['A', 'B', 'C', 'D']:
            f_df[item+'_p'] = f_df[item] / (f_df.shape[1]-4) # the 4 is the 4 items adding to the shape
            f_df = f_df.drop(columns=item)

        score = self.graded_df['SCORE']

        ret_df = f_df[['A_p','B_p','C_p','D_p']]
        f_df = f_df.drop(columns=['A_p','B_p','C_p','D_p'])
        # sloppy could be handled by a for loop but I am not operating at peak performance...
        a_df = f_df.replace('A',1)
        a_df = a_df.replace(['B','C','D'], 0)
        a_df = a_df.T
        a_df = a_df.apply(hfh.pd.to_numeric, errors='coerce')

        ret_df.loc[:, 'A_r'] = get_pbis(a_df, score)
        ret_df.loc[:, 'A_m'] = (a_df.T * score / self.operational_items).replace(0, nan).T.mean()

        b_df = f_df.replace('B', 1)
        b_df = b_df.replace(['A', 'C', 'D'], 0)
        b_df = b_df.T
        b_df = b_df.apply(hfh.pd.to_numeric, errors='coerce')
        ret_df.loc[:, 'B_r'] = get_pbis(b_df, score)
        ret_df.loc[:, 'B_m'] = (b_df.T * score / self.operational_items).replace(0, nan).T.mean()

        c_df = f_df.replace('C', 1)
        c_df = c_df.replace(['A', 'B', 'D'], 0)
        c_df = c_df.T
        c_df = c_df.apply(hfh.pd.to_numeric, errors='coerce')
        ret_df.loc[:, 'C_r'] = get_pbis(c_df, score)
        ret_df.loc[:, 'C_m'] = (c_df.T * score / self.operational_items).replace(0, nan).T.mean()

        d_df = f_df.replace('D', 1)
        d_df = d_df.replace(['A', 'B', 'C'], 0)
        d_df = d_df.T
        d_df = d_df.apply(hfh.pd.to_numeric,errors = 'coerce')
        ret_df.loc[:, 'D_r'] = get_pbis(d_df, score)
        ret_df.loc[:, 'D_m'] = (d_df.T * score / self.operational_items).replace(0, nan).T.mean()

        K_m = (self.graded_df.T * score / self.operational_items).replace(0, nan).T.mean()
        K_m = K_m.drop(['SCORE'])
        pbis = get_pbis(self.graded_df, score)

        self.graded_df = self.graded_df.drop(columns=['MARGINAL','SCORE'])
        mean = self.graded_df.mean()


        ret_df.loc[:, 'K_m'] = K_m
        ret_df.loc[:,'K_p'] = mean
        ret_df.loc[:,'K_r'] = pbis
        ret_df['N'] = self.graded_df.shape[0]
        ret_df = ret_df[['N','K_p','K_r','K_m','A_p','A_r','A_m','B_p','B_r','B_m','C_p','C_r','C_m','D_p','D_r','D_m']]
        return ret_df

    def process_item_stats(self, returns_lines = False, full_path = None):
        # this is intended to create an iteman like layout following the structure
        assert returns_lines is True or full_path is not None, \
            "process item stats has no purpose as it neither returns nor writes."

        '''
                AccNum  N   K_r    K_p      K_m                 p       r       m
                                                        A       x.xx    x.xx    x.xx
                                                        B       x.xx    x.xx    x.xx
                                                        C       x.xx    x.xx    x.xx
                                                        D       x.xx    x.xx    x.xx
                '''

        rows = self.ten_day_items.iterrows()
        lines = []

        for row in rows:
            key = get_key(row)
            line1 = row[0]
            line1 += ",N=" + str(row[1]['N'])
            line1 += ",K_r=" + str(hfh.c_round(row[1]['K_r']))
            line1 += ",K_p=" + str(hfh.c_round(row[1]['K_p']))
            line1 += ",K_m=" + str(hfh.c_round(row[1]['K_m']))
            line1 += ",,p,r,m"
            line1 += "\n"
            lines.append(line1)

            line2 = ",,,,,A,"
            line2 += str(row[1]['A_p']) + ','
            line2 += str(row[1]['A_r']) + ','
            line2 += str(row[1]['A_m'])
            line2 += "\n"
            lines.append(line2)


            line3 = ",,,,,B,"
            line3 += str(row[1]['B_p']) + ','
            line3 += str(row[1]['B_r']) + ','
            line3 += str(row[1]['B_m'])
            line3 += "\n"
            lines.append(line3)

            line4 = ",,,,,C,"
            line4 += str(row[1]['C_p']) + ','
            line4 += str(row[1]['C_r']) + ','
            line4 += str(row[1]['C_m'])
            line4 += "\n"
            lines.append(line4)

            line5 = ",,,,,D,"
            line5 += str(row[1]['D_p']) + ','
            line5 += str(row[1]['D_r']) + ','
            line5 += str(row[1]['D_m'])
            line5 += "\n"
            lines.append(line5)

            line6 = '\n'
            lines.append(line6)

        if full_path is not None:
            hfh.write_lines_to_text(lines, full_path)
        if returns_lines:
            return lines


def get_key(row, keys = ['A','B','C','D']):
    row = row[1]
    for key in keys:
        try:
            k_m = row['K_m']
            check1 = key + '_m'
            check1r = row[check1]
            check1 = close_enough(k_m, check1r)
            k_r = row['K_r']
            check2 = key + '_r'
            check2r = row[check2]
            check2 = close_enough(k_r, check2r)
            k_p = row['K_p']
            check3 = key + '_p'
            check3r = row[check3]
            check3 = close_enough(k_p, check3r)

            if check1 and check2 and check3:
                return key
        except:
                print("invalid")
    print("no key found")
    #assert False, "no key found"

def close_enough(a,b,tolerance = .01):
    if abs(a-b)<tolerance:
        return True
    return False

def get_alpha(graded_df):
    N = graded_df.shape[1]
    all_correct = graded_df.min().mean() == 1
    if all_correct:
        return 1
    else:
        item_var = graded_df.var()
        item_var = item_var.sum()
        score_var = graded_df.sum(axis=1)
        score_var = score_var.var()
        alpha = (N / (N - 1)) * (1 - item_var / score_var)
        return alpha

def get_pbis(dichotomous_df, score = None, adjust_score = False):
    # assumes that if score is None score is sum of axis

    if score is None:
        score = dichotomous_df.sum(axis = 1)
    cols = dichotomous_df.iteritems()
    rs = []
    ids = []
    _score = score
    for col in cols:
        if adjust_score:
            _score = score - col[1]
        r = col[1].corr(_score)
        rs.append(r)
        ids.append(col[0])
    ret = hfh.pd.Series(rs, index=ids)
    return ret