import core.h_file_handling as hfh
import core.h_passing_score as hp


class Plan:
    def __init__(self, project_path, name = None, populate = True):

        self.log = ""
        self.name = name
        self.project_path = project_path
        self.layers = []
        # defined by plan... not an argument.
        self.theta = None
        self.target_cut = None

        #add needed info to final statistics
        target_cols = ['Content Area']
        bank_path = hfh.get_single_file(project_path + '/bank_files',target_string='BANK')
        stats_path = hfh.get_single_file(project_path + '/calibration/final', target_string = 'CULLED')
        hfh.add_info_from_bank_to_csv(
            bank_path=bank_path,csv_path=stats_path,list_of_target_columns=target_cols,replace_csv=True,convert_to_int_str=False)

        self.parse_plan(project_path)
        if populate:
            self.form = self.populate_form()

            p = str(self.get_passing_for_form())[2:4]

            count = 0
            while p != str(int(self.target_cut*100)):
                count+=1
                self.form = self.populate_form()
                p = str(self.get_passing_for_form())[2:4]
            print(count)
            theta_name = str(self.theta).replace('.','-')
            name = project_path + '/forms/generated_forms/' + self.name + '_CUT_' + p + '%@THETA_' + theta_name+'_L.csv'
            self.form.to_csv(name, index = False)


        print("hello")

    def summary_of_form(self, target = 'Domain'):
        true_target = self.form[target]
        ret = self.form.groupby(by = [target]).count()
        count = ret.iloc[:,0]
        ret = self.form.apply(hfh.pd.to_numeric, errors = 'coerce')
        ret[target] = true_target
        ret = ret.dropna(axis=1)
        ret = ret.groupby(by = [target]).mean()[['IN_DEV_EVAL','OUT_DEV_EVAL','B_EVAL','b','POINT_BISERIAL_T']]
        ret['COUNT'] = count
        a = hfh.pd.unique(self.form['AccNum'])
        test = self.form.groupby(by = [target]).nunique()
        ret['UNIQUE']=test['AccNum']
        if len(a) != len(self.form):
            print("ERROR: repeated AccNum")
            return False
        return ret


    def parse_plan(self,project_path):
        PARAMETER = '@'
        LAYER_DEF = '!'
        P_THETA = 't'
        P_CUT = '%'
        plan_path = hfh.get_single_file(project_path + '/exam_plan', target_string='PLAN')
        lines = hfh.get_lines(plan_path, ignore_pound=True, ignore_blank_lines=True)
        if self.name is None:
            self.name = hfh.get_stem(plan_path)

        for count in range(len(lines)-1):
            line = lines[count].strip()
            first_character = line[0]
            if first_character == PARAMETER:
                if line[1] == P_THETA:
                    try:
                        self.theta = float(line[2:])
                    except:
                        m = "THETA can not be cast to float", line[2:]
                        self.log += m + '\n'
                        print(m)
                if line[1] == P_CUT:
                    try:
                        self.target_cut = float(line[2:])/100
                    except:
                        m = "CUT_SCORE can not be cast to float", line[2:]
                        self.log += m + '\n'
                        print(m)
            if first_character == LAYER_DEF:
                hierarchy = line.split('!')[1:]
                distributions = []
                completing = True
                i = 0
                while completing:
                    i+=1
                    if len(lines)>count+i:
                        next_line = lines[count+i].strip()
                        first_character = next_line[0]
                        if first_character != LAYER_DEF:
                            distribution = next_line.split(',')
                            distributions.append(distribution)
                        else:
                            completing = False
                            self.layers.append(Layer(hierarchy,distributions))
                    else:
                        completing = False
                        self.layers.append(Layer(hierarchy, distributions))

    def populate_form(self):
        final_stats_path = hfh.get_single_file(self.project_path+'/calibration/final', 'CULLED')
        bank_path = hfh.get_single_file(self.project_path + '/bank_files', 'BANK')
        bank_df = hfh.pd.read_excel(bank_path)
        final_stats_df = hfh.get_df(final_stats_path, header =0 )

        #todo: sloppy 9999 came in from somewhere and needs to go away

        #final_stats_df = final_stats_df.dropna(subset=['Domain'])
        #final_stats_df['Domain'] = hfh.pd.to_numeric(final_stats_df['Domain']).astype(int).astype(str)
        #final_stats_df['Content Area'] = bank_df['Content Area']
        #final_stats_df = final_stats_df.dropna(subset=['Content Area'])

        #final_stats_df['Content Area'] = bank_df['Content Area'].astype(int).astype(str)

        selected_items = hfh.pd.DataFrame([])
        #self, used_items, available_items, target_cut_score , passing_theta):
        for layer in self.layers:
            #todo add code to ensure layer data is here.
            selected_items = layer.select_items_for_layer(selected_items, final_stats_df, self.target_cut, self.theta)
        return selected_items


    def get_passing_for_form(self):
        if self.form.shape[0]>0:
            p = hp.get_passing_from_bs_and_theta(self.form['b'],self.theta)
            self.log += "form p = " + str(p) + '\n'
            return p
        return 0

    def create_log(self):
        pass


class Layer:
    def __init__(self, hierarchy, distributions, by_n = True):
        #   hierarchy is a list of target columns
        #   e.g.
        #   [Domain, Task]
        #   distribution is a list of assigned items in hierarchy
        #   by_n asserts the distribution is by item. False means distribution is by percentage

        #   if a plan has more than one layer the items from one layer do not satisfy the requirements of other layers

        self.hierarchy = hierarchy
        self.distributions = distributions
        self.by_n = by_n
        self.accNums = []

    def select_items_for_layer(self, used_items, available_items, target_cut_score , passing_theta):
        #example hierarchy = ['Project','Domain]
        #example distribution = ['02 ORCA BAY',1,2]
        PRECISION = .005
        P_DIFF = 0.0
        P_SAMPLE = P_DIFF*2
        selected_items = used_items
        for distribution in self.distributions:
            possible_items = available_items.drop(used_items.index)
            possible_items['b'] = possible_items['b'].astype(float)
            n_i = 0
            for h in self.hierarchy:
                if h.find('~') > -1:
                    exclude = h[1:]
                    possible_items = possible_items[hfh.pd.isnull(possible_items[exclude])]
                    n_i=-1

            for h in range(len(self.hierarchy)):
                # look for ~ which excludes any items with a value for that index

                target = self.hierarchy[h]
                if target[0] != '~':
                    value = distribution[h]
                    possible_items = possible_items[possible_items[target]==value]

            n = int(distribution[len(self.hierarchy)+n_i])

            if possible_items.shape[0]<n:
                print("impossible request")
            else:
                p = target_cut_score
                diff_n = 0
                diff_items = hfh.pd.DataFrame([])
                if len(selected_items)>0:
                    p = hp.get_passing_from_bs_and_theta(selected_items['b'], passing_theta)

                if abs(p-target_cut_score)>PRECISION:
                    diff_n = int(n*P_DIFF)
                    diff_items = self.select_by_difficuly(possible_items,target_cut_score,p,diff_n,n)

                r_n = n-diff_n
                possible_items = possible_items.drop(diff_items.index)

                r_items = possible_items.sample(r_n)

                if len(diff_items)>0:
                    selected_items = hfh.pd.concat([selected_items, diff_items])
                if len(r_items)>0:
                    selected_items = hfh.pd.concat([selected_items, r_items])

        return selected_items


    def select_by_difficuly(self, available_items, target_cut_score, p, diff_n, n):
        P_DIFF = .3
        P_SAMPLE = .5
        PRECISION = .005

        possible_items = available_items
        possible_n = len(possible_items)
        subset_n = int(P_SAMPLE * possible_n)
        select_n = int(P_DIFF * n)

        ascending = False
        if p < target_cut_score * (1 - PRECISION):
            ascending = True

        if select_n>possible_n:
            return possible_items

        else:
            possible_items = possible_items.sample(subset_n)
            possible_items = possible_items.sort_values(by='b', ascending=ascending)
            possible_items = possible_items.head(select_n)
            diff_items = possible_items.head(diff_n)
            return diff_items