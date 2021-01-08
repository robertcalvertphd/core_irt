class E:
    #   report names end in _R
    REMOVED_ITEMS_R = "REMOVED_ITEMS.csv"
    CULLED_ITEMS_R = "CULLED.csv"
    COMPLETE_ITEMS_R = "COMPLETE.csv"
    PASSING_REPORT_R = "PASSING_REPORT.csv"

    #   folder names end in _P
    BACKUP_PROCESSED_DATA_P = "backup_processed_data"
    BANK_P = "bank_files"
    REPORTS_P = "reports"
    PROCESSED_DATA_P = "processed_data"
    CALIBRATION_P = "calibration"
    INITIAL_CALIBRATION_P = CALIBRATION_P + '/initial'
    FINAL_CALIBRATION_P = CALIBRATION_P + '/final'
    XCALIBRE_P = '/xCalibre'
    INITIAL_XCALIBRE_P =XCALIBRE_P + '/initial'
    FINAL_XCALIBRE_P = XCALIBRE_P + '/final'
    FORMS_P = '/forms'
    OPERATIONAL_FORMS_P = FORMS_P + '/operational'
    CROSS_VALIDATION_P = '/cross_validation'

    # header
    C_HEADER_L = ["AccNum", "Key","Options","Domain","Include","Type"]
    C_HEADER_S = "AccNum,Key,Options,Domain,Include,Type"

    # file name
    TRAINING_GROUP_F = "_TRAINING_GROUP_f.csv"
    VALIDATION_GROUP_F = "_VALIDATION_GROUP_f.csv"