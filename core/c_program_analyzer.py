import core.h_passing_score as hp
import core.h_form_generation as hf
import core.h_reports as rep
import core.h_drift as hd
import core.h_cross_validation as hc

hfh = hp.hfh
hr = hp.hr
os = hfh.os
E = hfh.E

#   the purpose of this class is to consolidate the functions performed for a program into a single class
#   which can assess its capacity and validate steps along the way


class ProgramAnalyzer:
    INITIAL = 90  # describes calibration state
    FINAL = 91  # describes calibration state

    def __init__(self, parent_path, name, theta = None, lxr = False):
        self.lxr = lxr
        self.theta = theta
        self.name = name
        self.program_path = parent_path + '/' + name
        if os.path.isdir(self.program_path) is False:
            hfh.create_dir(self.program_path)
        self.log_entries = []
        self.has_folders = self.folders_are_valid()
        self.has_processed = self.has_processed_data()
        self.has_xCalibre = self.has_xCalibre_data(type=ProgramAnalyzer.INITIAL)
        self.has_xCalibre = self.has_xCalibre_data(type=ProgramAnalyzer.FINAL)
        self.has_plan = self.has_plan_data()
        self.has_calibration = self.has_calibration_data()
        self.has_bank_files = self.has_bank_data()
    #    self.grade_exams()
        '''
        if not self.has_processed and self.has_bank_files:
            self.process_raw_data()
        
        if self.has_bank_files:
            self.create_forms()
        '''

    def folders_are_valid(self):
        try:
            self.make_folders()
            return 1
        except:
            return 0

    def make_folders(self):
            project_folder = self.program_path

            hfh.create_dir(project_folder + "/raw_data")
            hfh.create_dir(project_folder + "/processed_data")
            hfh.create_dir(project_folder + "/calibration")
            hfh.create_dir(project_folder + "/calibration/initial")
            hfh.create_dir(project_folder + "/calibration/final")
            hfh.create_dir(project_folder + "/reports")
            hfh.create_dir(project_folder + "/xCalibre")
            hfh.create_dir(project_folder + "/xCalibre/initial")
            hfh.create_dir(project_folder + "/xCalibre/final")
            hfh.create_dir(project_folder + '/cross_validation')
            hfh.create_dir(project_folder + '/forms')
            hfh.create_dir(project_folder + "/bank_files")
            hfh.create_dir(project_folder + "/exam_plan")
            hfh.create_dir(project_folder + "/graded")
            hfh.create_dir(project_folder + "/forms/generated_forms")
            hfh.create_dir(project_folder + "/forms/operational")
            hfh.create_dir(project_folder + "/forms/full")
            hfh.create_dir(project_folder + "/_INBOX")
            hfh.create_dir(project_folder + "/" + hfh.E.BACKUP_PROCESSED_DATA_P)

    def has_processed_data(self):
        processed_path = self.program_path + '/processed_data'
        if os.path.isdir(processed_path) is False:
            return False
        f_files = hfh.get_all_file_names_in_folder(processed_path, target_string='_f.csv')
        c_files = hfh.get_all_file_names_in_folder(processed_path, target_string='_c.csv')
        self.processed_pairs = hfh.pair_files(f_files, c_files, pair_full=True)

        if len(self.processed_pairs) == 0:
            return False
        return True

    def log(self, message):
        self.log_entries.append(message)

    def log_missing(self, name):
        self.log("error identifying exactly one " + name + ' file')

    def has_xCalibre_data(self, type = INITIAL):
        type_s = False
        if type == ProgramAnalyzer.INITIAL:
            type_s = 'initial'
        if type == ProgramAnalyzer.FINAL:
            type_s = 'final'
        assert type_s is not False, "invalid argument in has_xCalibre_data. Type must be INITIAL or FINAL constant."
        xCalibre_path = self.program_path + '/xCalibre/' + type_s
        stats = hfh.get_single_file(xCalibre_path, target_string='Stats')
        tif = hfh.get_single_file(xCalibre_path, target_string='TIF')
        iif = hfh.get_single_file(xCalibre_path, target_string='IIF')
        matrix = hfh.get_single_file(xCalibre_path, target_string='Matrix')

        if stats is False:
            self.log_missing(type_s + "stats")
        if tif is False:
            self.log_missing(type_s + "tif")
        if iif is False:
            self.log_missing(type_s + "iif")
        if matrix is False:
            self.log_missing(type_s + "matrix")
        if stats and tif and iif and matrix:
            return type

    def has_plan_data(self):
        exam_plan_path = self.program_path + '/exam_plan'
        plan = hfh.get_single_file(exam_plan_path,target_string=".")
        if plan:
            #p = hf.Plan(self.program_path)
            #self.theta = p.theta
            return True
        self.log_missing("exaam plan")
        return False

    def has_calibration_data(self):
        calibration_folder = self.program_path + '/calibration'
        #   calibration should contain both an INITIAL c and f and a FINAL c and f
        initial_f = hfh.get_single_file(calibration_folder+'/initial', '_f.csv')
        initial_c = hfh.get_single_file(calibration_folder+'/initial', '_c.csv')
        final_f = hfh.get_single_file(calibration_folder+'/final', '_f.csv')
        final_c = hfh.get_single_file(calibration_folder+'/final', '_c.csv')

        if not initial_c:
            self.log_missing("INITIAL_c")
        if not initial_f:
            self.log_missing("INITIAL_f")
        if not final_c:
            self.log_missing("FINAL_c")
        if not final_f:
            self.log_missing("FINAL_f")
        ret = False
        if initial_f and initial_c and not final_c and not final_f:
            ret = ProgramAnalyzer.INITIAL
        if final_f and final_c:
            return ProgramAnalyzer.FINAL
        return ret

    def has_bank_data(self):
        # bank should contain a file which matches each data name exactly and a BANK file
        bank_path = self.program_path + '/bank_files'
        raw_data_path = self.program_path + '/raw_data'
        bank_files = hfh.get_all_file_names_in_folder(bank_path)
        data_files = hfh.get_all_file_names_in_folder(raw_data_path)
        full_bank = hfh.get_single_file(bank_path, target_string='BANK')
        match = False
        if len(bank_files) == len(data_files) + 1:
            match = True
        if not match:
            self.log("bank files do not match data files")
        if not full_bank:
            self.log_missing("BANK file")
        if match and full_bank:
            return True
        return False

    def create_forms(self, LXR = False):
        if LXR is False:
            hr.create_forms_from_bank(self.program_path, operational=False)
            hr.create_forms_from_bank(self.program_path, operational=True)
        else:
            c_files = hfh.get_all_files(self.program_path + '/processed_data', target_string='_c.csv')
            for file in c_files:
                hr.create_form_from_c(file,self.program_path+'/forms/operational')
            self.log("FORMS ARE CREATED USING LXR AND MAY INCLUDE PRETEST ITEMS.")

    def process_raw_data(self, from_bank = True):
        if from_bank:
            assert self.has_bank_files, "MISSING BANK FILES. REQUIRES EXACT MATCH BETWEEN bank and data, and a BANK file"
        bank_files = hfh.get_all_files(self.program_path + '/' + E.BANK_P, extension='xlsx')
        for b in bank_files:
            if b.find('BANK') == -1:
                name = hfh.get_stem(b)
                c_df = hr.create_c_df_from_bank(b)
                c_df.to_csv(self.program_path + '/' + E.PROCESSED_DATA_P + '/' + name + '_c.csv', index = False)
        raw_data = self.program_path + '/raw_data'
        data_files = hfh.get_all_file_names_in_folder(raw_data)
        processed_files = hfh.get_all_file_names_in_folder(self.program_path+'/processed_data')
        if self.lxr:
            hr.create_c_from_LXR_Test(self.program_path+'/bank_files',self.program_path + '/processed_data')
        if not from_bank and not self.lxr:
            for file in data_files:
                hr.process_response_string_file(file, write_csv=True,destination_path= self.program_path + '/processed_data', create_c=False)
        if from_bank and not self.lxr:
            #todo: this is problematic... I am going to do a full rework which may break things.

            bank_files = hfh.get_all_file_names_in_folder(self.program_path+'/bank_files')
            pairs = hfh.pair_files(bank_files, data_files, pair_full=True)
            if len(pairs)>0:
                self.log("Paired " + str(len(pairs))+" files for processing.")
            else:
                self.log("Failed to pair raw data files and bank files.")
            for pair in pairs:
                if pair[1] not in processed_files:
                    hr.process_response_string_file(pair[1],
                                                    pair[0],
                                                    destination_path=self.program_path+'/processed_data',
                                                    write_csv=True,
                                                    create_c=False
                                                    )
#        self.create_CAL(ProgramAnalyzer.INITIAL)

    def setup_cross_calibration(self):
        hc.create_cross_validation_data(self.program_path)

    def handle_drift(self):
        hd.run(self.program_path, self.name)

    def create_CAL(self, stage):
        assert stage == ProgramAnalyzer.INITIAL or ProgramAnalyzer.FINAL, \
            "create_CAL stage issue either FINAL or INITIAL"
        stage_path = 'final'
        if stage == ProgramAnalyzer.INITIAL:
            stage_path = "initial"

        path = self.program_path+'/calibration/'+stage_path
        print("creating calibration file")
        hr.create_CAL(self.program_path, destination_path = path, pair_full = True)
        # update to remove drift
        #hd.run(self.name,self.program_path)

    def remove_all_but_in_use_from_f_and_c(self,f_df:hfh.pd.DataFrame, c_df:hfh.pd.DataFrame):
        bank_path = self.program_path+'/bank_files'
        full_bank_path = hfh.get_single_file(bank_path, target_string='BANK', strict=True)
        bank_df = hfh.pd.read_excel(full_bank_path, header = 0)
        bank_df = bank_df.set_index('AccNum')
        f_df = f_df.T
        f_df = f_df.set_index(c_df[0])
        c_df = c_df.set_index(c_df[0])
        c_df = c_df.drop(columns=[0])
        c_df['ItemStatus'] = (bank_df['ItemStatus']=='InUse') | (bank_df['ItemStatus']=='unused')
        f_df['ItemStatus'] = c_df['ItemStatus']

        f_df = f_df.dropna(subset=['ItemStatus'])
        c_df = c_df.dropna(subset=['ItemStatus'])
        c_df = c_df[c_df['ItemStatus']]
        f_df = f_df[f_df['ItemStatus']]
        f_df = f_df.drop(columns=['ItemStatus'])
        c_df = c_df.drop(columns=['ItemStatus'])

        return f_df.T, c_df

    def create_culled(self, type):
        assert type == ProgramAnalyzer.INITIAL or type == ProgramAnalyzer.FINAL, \
            'create_culled requires canonical constant as type.'
        type_s ='final'
        if type == ProgramAnalyzer.INITIAL:
            type_s = 'initial'
        rep.check_assumptions(self.program_path, type_s, name = self.name,bank_comparison=False)
#       rep.update_removed_item_report_from_culled_and_complete(program_path=self.program_path)
#   todo: fix this
        print("updating removed items from assumptions doesn't work yet... don't believe that it does.")

    def create_final_calibration(self):
        # check for required files
        culled = hfh.get_single_file(self.program_path+'/calibration/initial', target_string="CULLED",as_df=True, header = 0)
        # add in possible columns used for creation

        f_path = hfh.get_single_file(self.program_path+'/calibration/initial', target_string = '_f.csv', index_col=0)
        f_df = hfh.get_df(f_path, index_col=0)
        c_df = hfh.get_single_file(self.program_path+'/calibration/initial', target_string = '_c.csv', as_df=True)

        if culled is not False and f_df is not False and c_df is not False:
            f_df = f_df.T
            f_df = f_df.reset_index(drop=True)
            keep = c_df[0].isin(culled['AccNum'])
            f_df = f_df[keep]
            c_df = c_df[keep]
            f_df = f_df.T
            f_df.to_csv(self.program_path+'/calibration/final/'+self.name+'_FINAL_f.csv', header=False)
            c_df.to_csv(self.program_path + '/calibration/final/' + self.name + '_FINAL_c.csv',header=False, index=False)

    def get_passing_for_operational_forms(self, target_form = None):
        assert self.theta is not None, "THETA MUST BE SET TO GET PASSING FOR FORMS."
        if target_form is None:
            forms = hfh.get_all_file_names_in_folder(self.program_path + '/forms/operational')
            for form in forms:
                #   todo changed for testing change back if not accepted
                #hp.get_passing_form_info(self.program_path, form)
                hp.create_passing(self.program_path,form,self.theta,useComplete=True)
        if target_form is not None:
            hp.create_passing(self.program_path, target_form, self.theta)

    def create_exam(self, plan = None, name = "_"):
        exam_plan_path = plan
        if plan is None:
            exam_plan_path = hfh.get_single_file(self.program_path+'/exam_plan', target_string='.txt')
        assert exam_plan_path is not None, "no exam plan for " + self.name + " and exam construction requested"
        assert self.theta is not None, "no theta set for " + self.name + " and exam construction requested"

        a = hf.Plan(self.program_path,name)
        sum = a.summary_of_form()
        print(sum)

    def separate_calibration(self):
        c_df = hfh.get_single_file(self.program_path+'/calibration/initial', target_string='_c',as_df=True)
        f_df = hfh.get_single_file(self.program_path + '/calibration/initial', target_string='_f', as_df=True, index_col=0)

        hfh.create_dir(self.program_path+'/calibration/initial/separated')
        f_df = f_df.T
        f_df = f_df.set_index(c_df[0])
        c_df = c_df.set_index(c_df[0])
        f_df1 = f_df.sample(frac=.5)
        c_df1 = c_df[c_df.index.isin(f_df1.index)]
        f_df2 = f_df[~f_df.index.isin(f_df1.index)]
        c_df2 = c_df[c_df.index.isin(f_df2.index)]
        #f_df_2 = f_df[~f_df_1]
        #c_df_2 = c_df[f_df_2]
        print("hello")

    def move_f_from_processed_to_raw(self):
        f_files = hfh.get_all_files(self.program_path + '/processed_data', target_string='_f')
        for file in f_files:
            raw_path = self.program_path + '/raw_data'
            name = hfh.get_stem(file)[:-2] + '.' + hfh.get_extension(file)
            destination = raw_path + '/' + name
            hfh.move_file(file, destination)

    def grade_exams(self, write_csv = False, strict = False, debug = True):
        if debug:
            print("grading started")
        # mostly used to throw assertion flags early.
        # strict means that what is being fed in is already strictly formatted (this is most often not the case.
        f_files = hfh.get_all_files(self.program_path + '/processed_data', target_string='_f')
        c_files = hfh.get_all_files(self.program_path + '/processed_data', target_string='_c')
        pairs = hfh.pair_files(f_files, c_files,pair_full=True)
        graded_path = self.program_path + "/graded/"
        if strict and debug:
            print("STRICT IS true in grade_exams so headers are assumed.")
        for pair in pairs:
            print("grading started for form " + pair[0])
            graded_file = hfh.get_stem(pair[0])[:-2]+'_g.csv'
            f_df = hfh.get_df(pair[0])

            if strict and debug:
                c_df = hfh.get_df(pair[1], header = 0)
            else:
                c_df = hfh.get_df(pair[1], header=None)
            if not strict:
                strict_ret = hr.get_strict_format_f_df(c_df, f_df, get_c_df=True)
                c_df = strict_ret[0]
                f_df = strict_ret[1]

            g_df = hr.strict_grade(c_df, f_df,correct=1, incorrect=0, operational=False)
            mean = g_df.mean().mean()
            if write_csv:
                g_df.to_csv(graded_path+graded_file)
            if debug:
                print("grading finished for " + pair[0] +
                      " average score was " + hfh.c_round(mean, as_string=True, as_percentage=True))

    def create_passing_report(self):
        rs = [hfh.c_round(self.theta-.1), hfh.c_round(self.theta), hfh.c_round(self.theta + .1)]
        df = rep.get_passing_report(self.program_path, rs)
        path = self.program_path + "/" + E.REPORTS_P + '/' + E.PASSING_REPORT_R
        df.to_csv(path, header=False)

    def compare_to_bank_for_sanity_check(self, fatal = False):
        bank_path = self.program_path + '/' + E.BANK_P
        full_bank = hfh.get_single_file(bank_path, target_string='BANK')
        bank_df = hfh.pd.read_excel(full_bank)
        calibration_path = self.program_path + '/' + E.FINAL_XCALIBRE_P
        stats_path = hfh.get_single_file(calibration_path, target_string='Stats')
        if stats_path is False:
            print(self.name + ' does not have a final calibration. Initial calibration is being checked')
            calibration_path = self.program_path + '/' + E.INITIAL_XCALIBRE_P
            stats_path = hfh.get_single_file(calibration_path, target_string='Stats')
            if stats_path is False:
                if fatal :
                    assert stats_path is not False, "compare bank check could not locate stats. Fatal Error."
                else:
                    print("Compare bank check could not locate stats. Aborting.")
        stats_df = hfh.get_stats_df(stats_path)
        stats_df = stats_df.set_index('Item ID')
        bank_df = bank_df.set_index('AccNum')
        stats_df = stats_df.merge(bank_df, left_index=True, right_index=True)
        p_r = stats_df['P Value'].corr(stats_df['P'])
        p_b = stats_df['b'].corr(stats_df['P Value'])
        p_pbis = stats_df['S-Rpbis'].corr(stats_df['cPBS'])
        out = stats_df[['P Value', 'S-Rpbis', 'b', 'P', 'cPBS']]
        out.to_csv("test.csv")
        print("hello")