#' Searches and extracts data fields that have multiple records and adds a date for the data collection which is in separate data-field, current version works only with very specific UK Biobank data
#'
#' @param a list of the number of times data was collected
#' @param b type of data, in the future to be replaced by imputed field-ids
#' @param c dataframe containing date of code
#' @param d dataframe containing code codes
#' @param e name of data source
#' @param f dataframe containing date of assessment centre
#' @return A dataframe containing of extracted data, each data item is a row with participant ID, code and source
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data

SR_data_out <-  function(a,b,c,d,e,f) {
  . <- NULL
  if (b=="NC") {
    field_id_date <- paste0("^20008-",a)
    field_id_code <- paste0("^20002-",a)
  } else if (b=="OP") {
    field_id_date <- paste0("^20010-",a)
    field_id_code <- paste0("^20004-",a)
  }

  field_id_assesment <- paste0("^53-",a)
  field_id_assesment_selection <- paste0("53-",a,".0")

  SR_dates <- c %>%
    dplyr::select(1,tidyselect::matches(field_id_date)) %>%
    tidyr::pivot_longer(c(2:length(colnames(.))),names_to = "type",values_to = "year_dx") %>%
    tibble::rowid_to_column() %>%
    tidyr::drop_na() %>%
    dplyr::select(.data$rowid,.data$year_dx)

  SR_visit <- d %>%
    dplyr::select(1,tidyselect::matches(field_id_code)) %>%
    tidyr::pivot_longer(c(2:length(colnames(.))),names_to = "type",values_to = "code") %>%
    tibble::rowid_to_column() %>%
    tidyr::drop_na() %>%
    dplyr::left_join(dplyr::select(f,1,tidyselect::matches(field_id_assesment))) %>%
    dplyr::left_join(SR_dates,by=c("rowid")) %>%
    dplyr::select(.data$eid,.data$code,date_of_dx=.data$year_dx,date_of_visit={{field_id_assesment_selection}}) %>%
    dplyr::mutate(date_of_dx = ifelse(.data$date_of_dx == -1, NA, ifelse(.data$date_of_dx == -3, NA, .data$date_of_dx)),
                  date_of_dx = lubridate::date_decimal(.data$date_of_dx) %>%
                      (lubridate::round_date)(unit="day") %>%
                      parse_date_if_needed(),
                  date_of_visit = parse_date_if_needed(.data$date_of_visit),
                  date = dplyr::coalesce(date_of_dx, date_of_visit),
                  source=e,
                  code=as.character(.data$code)) %>%
    dplyr::select(.data$eid,.data$code,.data$date,.data$source)

  return(SR_visit)

}

#' Creates a symmetrical version of setdiff
#'
#' @param vec1 vector 1
#' @param vec2 vector 2
#' @author DIZtools
#' @return a vector of differences
#' @keywords internal
#' @keywords internal


setdiff_all <- function(vec1, vec2) {
  return(setdiff(union(vec1, vec2), intersect(vec1, vec2)))
}

#' Manipulates data from UK Biobank and creates files that are used in the later stages of phenotype development.
#'
#' @param min_data Full path of the file generated by the previous step (01_minimum_data.R).
#' @param GPC Full path of the primary care clinical data from UK Biobank.
#' @param GPP Full path of the primary care prescription data from UK Biobank.
#' @param hesin_diag Full file path of hesin_diag file from UK-Biobank.
#' @param HESIN Full file path of HESIN file from UK-Biobank.
#' @param hesin_oper Full file path of the hesin_oper file from UK-Biobank.
#' @param death_cause Full file path of the death_cause file from UK-Biobank.
#' @param death Full file path of the death file from UK-Biobank contains date of death info.
#' @param exclusions Full path of the file containing individuals to be excluded from the analysis. Defaults behaviour is to retain all individuals.
#' @param king_coef Full file path of related data file containing King coefficient scores for related ID pairs from UK-Biobank.
#' @param save_location Full file path for the common folder to save created files.
#' @importFrom magrittr %>%
#' @return Returns a series of files dependent on the inputs.
#' @export
#'
data_preparation_R <- function(min_data,
                               GPC,
                               GPP,
                               hesin_diag,
                               HESIN,
                               hesin_oper,
                               death_cause,
                               death,
                               exclusions,
                               king_coef,
                               save_location) {
  . <- NULL

  # Evaluating arguments ----------------------------------------------------
  # save_loc
  if(!dir.exists(dirname(save_location))){
    rlang::abort(paste0("'save_loc' must be in an existing directory"))
  }


  # Load in and define variables -----------------------------------------------------------------
  # exclusions
  if(!is.null(exclusions)) {
    if(!file.exists(exclusions)){
      rlang::abort(paste0("'exclusions' must be a file"))
    }
    exclusions <- data.table::fread(exclusions, header = F) %>%
      dplyr::pull()
  } else {
    exclusions <- data.frame(V1=NA) %>%
      dplyr::pull()
  }
  # min_data
  if(!is.null(min_data)){
    if(!file.exists(min_data)){
      rlang::abort(paste0("'min_data' must be a file"))
    }
    tab_data <- data.table::fread(min_data) %>%
      dplyr::filter(!.data$eid %in% exclusions)
    available_tab_data <- colnames(tab_data)
  }
  # empty df
  all_phenotype_data <- data.frame(remove=NA)
  # the assessment centre date


  # UK Biobank self-reported non-cancer diagnosis  --------------------------------------
  # self-report non-cancer codes
  if(!is.null(min_data)) {
    if(length(stringr::str_which(available_tab_data,"^20002-"))>0 & length(stringr::str_which(available_tab_data,"^20008-"))>0 & length(stringr::str_which(available_tab_data,"^53-"))>0) {

      assesment_centre_date <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^53-"))
      SR_NC <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^20002-"))
      # the year of diagnosis
      SR_NC_year_dx <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^20008-"))
      # calculating N of visits
      SR_NC_visits <- as.numeric(unique(gsub(".*-(.+)\\..*","\\1",colnames(SR_NC_year_dx)[-1])))
      # function to process the data
      SR_NC_data <- mapply(SR_data_out,SR_NC_visits,MoreArgs=list(b="NC",c=SR_NC_year_dx,d=SR_NC,e="SR",f=assesment_centre_date), SIMPLIFY = F) %>%
        purrr::reduce(dplyr::bind_rows)
      # combine
      all_phenotype_data <- all_phenotype_data  %>%
        dplyr::bind_rows(SR_NC_data)
    }

    # UK Biobank self-report operations  -----------------------------------------------------------
    if(length(stringr::str_which(available_tab_data,"^20004-"))>0 & length(stringr::str_which(available_tab_data,"^20010-"))>0 & length(stringr::str_which(available_tab_data,"^53-"))>0) {
      assesment_centre_date <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^53-"))
      SR_OP <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^20004-"))
      # the date of operation
      SR_OP_year_dx <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^20010-"))
      # calculating N of visits
      SR_OP_visits <- as.numeric(unique(gsub(".*-(.+)\\..*","\\1",colnames(SR_OP_year_dx)[-1])))
      # function to process the data
      SR_OP_data <- mapply(SR_data_out,SR_OP_visits,MoreArgs=list(b="OP",c=SR_OP_year_dx,d=SR_OP,e="SROP",f=assesment_centre_date), SIMPLIFY = F) %>%
        purrr::reduce(dplyr::bind_rows)
      # combine
      all_phenotype_data <- all_phenotype_data  %>%
        dplyr::bind_rows(SR_OP_data)
    }
    # Cancer_registry ---------------------------------------------------------
    # split into date and diagnostic codes
    if(length(stringr::str_which(available_tab_data,"^40005-"))>0 & length(stringr::str_which(available_tab_data,"^40006-"))>0) {
      # date
      cancer_reg_date <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^40005-")) %>%
        tidyr::pivot_longer(c(2:length(colnames(.))),names_to = "type",values_to = "year_dx") %>%
        tibble::rowid_to_column() %>%
        tidyr::drop_na() %>%
        dplyr::select(.data$rowid,.data$year_dx)
      # data
      cancer_reg_data <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^40006-")) %>%
        dplyr::mutate_if(is.character, dplyr::na_if,"") %>%
        tidyr::pivot_longer(c(2:length(colnames(.))),names_to = "type",values_to = "code") %>%
        tibble::rowid_to_column() %>%
        tidyr::drop_na() %>%
        dplyr::left_join(cancer_reg_date, by=c("rowid")) %>%
        tidyr::drop_na() %>%
        dplyr::mutate(code=ifelse(nchar(.data$code) > 4, (stringr::str_extract(.data$code, "^.{4}")), .data$code),
                      date=parse_date_if_needed(.data$year_dx),
                      source="cancer") %>%
        dplyr::select(.data$eid,.data$code,.data$date,.data$source)
      # combine
      all_phenotype_data <- all_phenotype_data  %>%
        dplyr::bind_rows(cancer_reg_data)
    }
    # combined_sex ---------------------------------------------------------------------
    ## sex 1 = male, 0 = female
    if(length(stringr::str_which(available_tab_data,"^22001-"))>0 & length(stringr::str_which(available_tab_data,"^31-"))>0) {
      # genetic sex
      gen_sex <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^22001-")) %>%
        dplyr::rename(eid=1,gen_sex=2)
      # self-report sex
      reported_sex <- tab_data %>%
        dplyr::select(1,tidyselect::matches(("^31-"))) %>%
        dplyr::rename(eid=1,reported_sex=2)
      # dplyr::select genetic sex unless no genetic sex reported
      combined_sex <- gen_sex %>%
        dplyr::full_join(reported_sex) %>%
        dplyr::mutate(sex=ifelse(is.na(.data$gen_sex),.data$reported_sex,.data$gen_sex)) %>%
        tidyr::drop_na(.data$sex) %>%
        dplyr::filter(!.data$eid %in% exclusions) %>%
        dplyr::select(.data$eid,.data$sex)
      # write
      data.table::fwrite(combined_sex,paste0(save_location,"/combined_sex"),sep = "\t")
    }
    # Loss to follow-up --------------------------------------------------------
    if(length(stringr::str_which(available_tab_data,"^190-0.0"))>0) {
      loss_to_follow_up <- tab_data %>%
        dplyr::select(1,.data$`190-0.0`) %>%
        dplyr::filter(.data$`190-0.0`!=1) %>%
        dplyr::select(.data$eid)
      #write
      data.table::fwrite(loss_to_follow_up, paste0(save_location,"/control_exclusions"), na = NA, sep = "\t")
    }
    # Kinship_call_rate -------------------------------------------------------
    if(length(stringr::str_which(available_tab_data,"^22005-"))>0 & !is.null(king_coef)) {
      if(!file.exists(king_coef)){
        rlang::abort(paste0("'king_coef' must be a file"))
      }
      related <- data.table::fread(king_coef)
      related_expected_colname <- c("ID1","ID2","HetHet","IBS0","Kinship")

      if(!all(tibble::has_name(related,related_expected_colname))){
        warning(paste0("'king_coef' does not have the correct colnames and may not produce the correct output, expected colnames are:
                                 '"),paste(related_expected_colname, collapse=","),paste0("'
                                 not:
                                 "),paste(colnames(related), collapse=","),
                      paste0("
                                 differences between inputed file and expected are:
                                 "),paste(setdiff_all(names(related),related_expected_colname), collapse=","))

      }

      # KINGS coefficent
      related <- related %>%
        dplyr::select(.data$ID1,.data$ID2,.data$Kinship)
      # call rate
      call_r <- tab_data %>%
        dplyr::select(1,tidyselect::matches("^22005-")) %>%
        dplyr::rename(missingness=.data$`22005-0.0`)
      # combine to give missingness information to each of ID, this is done as when removing related individuals you remove the one with the highest missingness
      call_rate_kinship <- related %>%
        dplyr::left_join(call_r,by=c("ID1"="eid")) %>%
        dplyr::rename(missingness_ID1=.data$missingness) %>%
        dplyr::left_join(call_r,by=c("ID2"="eid")) %>%
        dplyr::rename(missingness_ID2=.data$missingness) %>%
        dplyr::filter(.data$Kinship>=0.0884) %>%
        dplyr::mutate(lower_missing = dplyr::if_else(.data$missingness_ID1 < .data$missingness_ID2, 1, 2)) %>%
        dplyr::filter(!.data$ID1 %in% exclusions,!.data$ID2 %in% exclusions)
      # write
      data.table::fwrite(call_rate_kinship,paste0(save_location,"/related_callrate"), sep = "\t")
    }
  }
  # GP data -----------------------------------------------------------------
  # simple edit to remove exclusions and format date
  if(!is.null(GPC)) {
    if(!file.exists(GPC)){
      rlang::abort(paste0("'GPC' must be a file"))
    }

    GP_C <- data.table::fread(GPC, na.strings = "")
    GPC_expected_colname <- c("eid","data_provider","event_dt","read_2","read_3","value1","value2","value3")

    if(!all(tibble::has_name(GP_C,GPC_expected_colname))){
      warning(paste0("'GPC' does not have the correct colnames and may not produce the correct output, expected colnames are:
                                 '"),paste(GPC_expected_colname, collapse=","),paste0("'
                                 not:
                                 "),paste(colnames(GP_C), collapse=","),
                    paste0("
                                 differences between inputed file and expected are:
                                 "),paste(setdiff_all(names(GP_C),GPC_expected_colname), collapse=","))

    }
    # GP clinical data
    GP_C <- GP_C %>%
      dplyr::filter(!.data$eid %in% exclusions) %>%
      dplyr::mutate(event_dt=parse_date_if_needed(.data$event_dt))
    # need to retain an edited copy as further phenotypes require this information
    data.table::fwrite(GP_C,paste0(save_location,"/GP_C_edit.txt.gz"), na = NA, quote = TRUE)
    # GP_ID file
    GP_ID <- GP_C %>%
      dplyr::distinct(.data$eid)
    data.table::fwrite(GP_ID,paste0(save_location,"/GP_C_ID.txt.gz"), na = NA, quote = TRUE)
    # split V2 and V3 as source
    GP_read_V2 <- GP_C %>%
      tidyr::drop_na(.data$read_2) %>%
      dplyr::mutate(source="V2") %>%
      dplyr::select(.data$eid,code=.data$read_2,date=.data$event_dt,.data$source) %>%
      tidyr::drop_na() %>%
      dplyr::mutate(date=as.Date(.data$date),code=as.character(.data$code),eid=as.integer(.data$eid), source=as.character(.data$source))
    GP_read_V3 <- GP_C %>%
      tidyr::drop_na(.data$read_3) %>%
      dplyr::mutate(source="V3") %>%
      dplyr::select(.data$eid,code=.data$read_3,date=.data$event_dt,.data$source) %>%
      tidyr::drop_na() %>%
      dplyr::mutate(date=as.Date(.data$date),code=as.character(.data$code),eid=as.integer(.data$eid), source=as.character(.data$source))
    # combine
    all_phenotype_data <- all_phenotype_data  %>%
      dplyr::bind_rows(GP_read_V3,GP_read_V2)
  }
  ## Need to edit GP_P data to make DMD codes a character for searching prescription data is used separately in the later functions and so is saved as an edited file
  if(!is.null(GPP)) {
    if(!file.exists(GPP)){
      rlang::abort(paste0("'GPP' must be a file"))
    }

    GP_P <- data.table::fread(GPP, na.strings = "")
    GPP_expected_colname <- c("eid","data_provider","issue_date","read_2","bnf_code","dmd_code","drug_name","quantity")

    if(!all(tibble::has_name(GP_P,GPP_expected_colname))){
      warning(paste0("'GPP' does not have the correct colnames and may not produce the correct output, expected colnames are:
                                 '"),paste(GPP_expected_colname, collapse=","),paste0("'
                                 not:
                                 "),paste(colnames(GP_P), collapse=","),
                    paste0("
                                 differences between inputed file and expected are:
                                 "),paste(setdiff_all(names(GP_P),GPP_expected_colname), collapse=","))

    }
    # GP prescription data
    GP_P <- GP_P %>%
      dplyr::filter(!.data$eid %in% exclusions) %>%
      dplyr::mutate(issue_date=parse_date_if_needed(.data$issue_date)) %>%
      dplyr::mutate(issue_date=format(.data$issue_date, "%Y-%m-%d"),
                    dmd_code=as.character(.data$dmd_code))
    # write
    data.table::fwrite(GP_P,paste0(save_location,"/GP_P_edit.txt.gz"), na = NA, quote = TRUE)
  }
  # HES ---------------------------------------------------------------------
  # HES data needs to be combined from 2 tables, HES_diag and HESIN, HESIN has dates, diag has codes
  if(!is.null(hesin_diag) & !is.null(HESIN)) {

    if(!file.exists(HESIN)){
      rlang::abort(paste0("'HESIN' must be a file"))
    }

    if(!file.exists(hesin_diag)){
      rlang::abort(paste0("'hesin_diag' must be a file"))
    }

    HESIN_table <- data.table::fread(HESIN)
    HESIN_expected_colname <- c("eid","ins_index","dsource","source","epistart","epiend","epidur","bedyear","epistat","epitype","epiorder","spell_index","spell_seq","spelbgin","spelend","speldur","pctcode","gpprpct","category","elecdate","elecdur","admidate","admimeth_uni","admimeth","admisorc_uni","admisorc","firstreg","classpat_uni","classpat","intmanag_uni","intmanag","mainspef_uni","mainspef","tretspef_uni","tretspef","operstat","disdate","dismeth_uni","dismeth","disdest_uni","disdest","carersi")

    if(!all(tibble::has_name(HESIN_table,HESIN_expected_colname))){
      warning(paste0("'HESIN' does not have the correct colnames and may not produce the correct output, expected colnames are:
                                 '"),paste(HESIN_expected_colname, collapse=","),paste0("'
                                 not:
                                 "),paste(colnames(HESIN_table), collapse=","),
                    paste0("
                                 differences between inputed file and expected are:
                                 "),paste(setdiff_all(names(HESIN_table),HESIN_expected_colname), collapse=","))

    }

    HESIN_diag <- data.table::fread(hesin_diag, na.strings = "")
    HESIN_diag_expected_colname <- c("eid","ins_index","arr_index","level","diag_icd9","diag_icd9_nb","diag_icd10","diag_icd10_nb")

    if(!all(tibble::has_name(HESIN_diag,HESIN_diag_expected_colname))){
      warning(paste0("'hesin_diag' does not have the correct colnames and may not produce the correct output, expected colnames are:
                                 '"),paste(HESIN_diag_expected_colname, collapse=","),paste0("'
                                 not:
                                 "),paste(colnames(HESIN_diag), collapse=","),
                    paste0("
                                 differences between inputed file and expected are:
                                 "),paste(setdiff_all(names(HESIN_diag),HESIN_diag_expected_colname), collapse=","))

    }

    HES <- HESIN_diag
    HES_dates <- HESIN_table %>%
      dplyr::select(.data$eid, .data$ins_index, .data$epistart,.data$admidate) %>%
      dplyr::mutate_if(is.character, dplyr::na_if,"") %>%
      dplyr::mutate(dated=ifelse((is.na(.data$epistart)==T & is.na(.data$admidate)==T), NA,ifelse(is.na(.data$epistart)==T, .data$admidate, .data$epistart))) %>%
      dplyr::mutate(dates=lubridate::as_date(.data$dated)) %>%
      dplyr::select(.data$eid, .data$ins_index, .data$dates) %>%
      tidyr::drop_na()
    # combine
    HES_all <- HES %>%
      dplyr::left_join(HES_dates) %>%
      tidyr::drop_na(.data$dates) %>%
      dplyr::mutate_if(is.character, dplyr::na_if,"") %>%
      dplyr::select(-.data$diag_icd9_nb,-.data$diag_icd10_nb,-.data$arr_index) %>%
      dplyr::filter(!.data$eid %in% exclusions) %>%
      dplyr::mutate(diag_icd10= ifelse(nchar(.data$diag_icd10) > 4, (stringr::str_extract(.data$diag_icd10, "^.{4}")), .data$diag_icd10),
                    diag_icd9= ifelse(nchar(.data$diag_icd9) > 5, (stringr::str_extract(.data$diag_icd9, "^.{5}")), .data$diag_icd9))
    # split by ICD10/9 and by poistion primary secondary and external (1,2,3)
    # Primary
    ICD10_HES_first <-  HES_all %>%
      dplyr::filter(.data$level==1) %>%
      dplyr::mutate(source="ICD10_1") %>%
      dplyr::select(.data$eid,code=.data$diag_icd10,date=.data$dates,.data$source,grouping_variable=.data$ins_index) %>%
      tidyr::drop_na(.data$code)
    # Secondary
    ICD10_HES_second <-  HES_all %>%
      dplyr::filter(.data$level==2) %>%
      dplyr::mutate(source="ICD10_2") %>%
      dplyr::select(.data$eid,code=.data$diag_icd10,date=.data$dates,source,grouping_variable=.data$ins_index) %>%
      tidyr::drop_na(.data$code)
    # External
    ICD10_HES_third <-  HES_all %>%
      dplyr::filter(.data$level==3) %>%
      dplyr::mutate(source="ICD10_3") %>%
      dplyr::select(.data$eid,code=.data$diag_icd10,date=.data$dates,.data$source,grouping_variable=.data$ins_index) %>%
      tidyr::drop_na(.data$code)
    # Primary
    ICD9_HES_first <-  HES_all %>%
      dplyr::filter(.data$level==1) %>%
      dplyr::mutate(source="ICD9_1") %>%
      dplyr::select(.data$eid,code=.data$diag_icd9,date=.data$dates,.data$source,grouping_variable=.data$ins_index) %>%
      tidyr::drop_na(.data$code) %>%
      dplyr::mutate(code=as.character(.data$code))
    # Secondary
    ICD9_HES_second <-  HES_all %>%
      dplyr::filter(.data$level==2) %>%
      dplyr::mutate(source="ICD9_2") %>%
      dplyr::select(.data$eid,code=.data$diag_icd9,date=.data$dates,.data$source,grouping_variable=.data$ins_index) %>%
      tidyr::drop_na(.data$code) %>%
      dplyr::mutate(code=as.character(.data$code))
    # External
    ICD9_HES_third <-  HES_all %>%
      dplyr::filter(.data$level==3) %>%
      dplyr::mutate(source="ICD9_3") %>%
      dplyr::select(.data$eid,code=.data$diag_icd9,date=.data$dates,.data$source,grouping_variable=.data$ins_index) %>%
      tidyr::drop_na(.data$code) %>%
      dplyr::mutate(code=as.character(.data$code))
    # combine
    all_phenotype_data <- all_phenotype_data  %>%
      dplyr::bind_rows(ICD10_HES_first,ICD10_HES_second,ICD10_HES_third,ICD9_HES_first,ICD9_HES_second,ICD9_HES_third)
  }
  # HES_op ------------------------------------------------------------------
  if(!is.null(hesin_oper)) {
    if(!file.exists(hesin_oper)){
      rlang::abort(paste0("'hesin_oper' must be a file"))
    }

    HES_op <- data.table::fread(hesin_oper)

    HES_op_expected_colname <- c("eid","ins_index","arr_index","level","opdate","oper3","oper3_nb","oper4","oper4_nb","posopdur","preopdur")

    if(!all(tibble::has_name(HES_op,HES_op_expected_colname))){
      warning(paste0("'HES_op' does not have the correct colnames and may not produce the correct output, expected colnames are:
                                 '"),paste(HES_op_expected_colname, collapse=","),paste0("'
                                 not:
                                 "),paste(colnames(HES_op), collapse=","),
                    paste0("
                                 differences between inputed file and expected are:
                                 "),paste(setdiff_all(names(HES_op),HES_op_expected_colname), collapse=","))

    }

    HES_op_data <- HES_op %>%
      dplyr::filter(!.data$eid %in% exclusions) %>%
      dplyr::left_join(HES_dates) %>%
      tidyr::drop_na(.data$dates) %>%
      dplyr::mutate(source="OPCS") %>%
      dplyr::select(.data$eid,code=.data$oper4,date=.data$dates,.data$source) %>%
      tidyr::drop_na(.data$code)
    # combine
    all_phenotype_data <- all_phenotype_data  %>%
      dplyr::bind_rows(HES_op_data)
  }
  # Mortality data ----------------------------------------------------------
  if(!is.null(death_cause) & !is.null(death)) {
    if(!file.exists(death_cause)){
      rlang::abort(paste0("'death_cause' must be a file"))
    }
    if(!file.exists(death)){
      rlang::abort(paste0("'death' must be a file"))
    }

    cause_of_death <- data.table::fread(death_cause)
    cause_of_death_expected_colname <- c("eid","ins_index","arr_index","level","cause_icd10")

    if(!all(tibble::has_name(cause_of_death,cause_of_death_expected_colname))){
      warning(paste0("'cause_of_death' does not have the correct colnames and may not produce the correct output, expected colnames are:
                                 '"),paste(cause_of_death_expected_colname, collapse=","),paste0("'
                                 not:
                                 "),paste(colnames(cause_of_death), collapse=","),
                    paste0("
                                 differences between inputed file and expected are:
                                 "),paste(setdiff_all(names(cause_of_death),cause_of_death_expected_colname), collapse=","))

    }

    date_of_death <- data.table::fread(death)
    date_of_death_expected_colname <- c("eid","ins_index","dsource","source","date_of_death")
    if(!all(tibble::has_name(date_of_death,date_of_death_expected_colname))){
      warning(paste0("'date_of_death' does not have the correct colnames and may not produce the correct output, expected colnames are:
                                 '"),paste(date_of_death_expected_colname, collapse=","),paste0("'
                                 not:
                                 "),paste(colnames(date_of_death), collapse=","),
                    paste0("
                                 differences between inputed file and expected are:
                                 "),paste(setdiff_all(names(date_of_death),date_of_death_expected_colname), collapse=","))

    }

    MD <- cause_of_death %>%
      dplyr::left_join(date_of_death) %>%
      dplyr::mutate(date_of_death=parse_date_if_needed(.data$date_of_death)) %>%
      tidyr::drop_na(.data$date_of_death)
    ## check to see if there is anyone with multiple dates of death
    MD_date_check <- MD %>%
      dplyr::group_by(.data$eid) %>%
      dplyr::summarise(differnce=max(.data$date_of_death)-min(.data$date_of_death)) %>%
      dplyr::filter(.data$differnce != 0) %>%
      dplyr::pull(.data$eid)
    # edit ICD10 codes the same as HES to allow maximum mapping and exclude the ids with multiple dates of deaths
    MD_data <- MD %>%
      dplyr::filter(!.data$eid %in% MD_date_check) %>%
      dplyr::filter(!.data$eid %in% exclusions)  %>%
      dplyr::mutate(code= ifelse(nchar(.data$cause_icd10) > 4, (stringr::str_extract(.data$cause_icd10, "^.{4}")), .data$cause_icd10),
                    source="MD") %>%
      dplyr::select(.data$eid,.data$code,date=.data$date_of_death,.data$source) %>%
      tidyr::drop_na(.data$code)
    # combine
    all_phenotype_data <- all_phenotype_data  %>%
      dplyr::bind_rows(MD_data)
  }
  ## combine and save
  all_phenotype_data <- all_phenotype_data %>%
    dplyr::select(-.data$remove)
  data.table::fwrite(all_phenotype_data,paste0(save_location,"/health_records.txt.gz"),sep = "\t",na = NA)
  # Control populations -----------------------------------------------------
  if(all(c("combined_sex","GP_C_ID.txt.gz") %in% list.files(save_location))){
    # making populations from combined sex (as should eb everybody)
    combined_sex_edit <- data.table::fread(paste0(save_location,"/combined_sex")) %>%
      dplyr::mutate(all_pop=1) %>%
      dplyr::select(.data$eid,.data$all_pop)
    # and GP data (required to make effect controls and minimise misclassification of controls)
    GP_ID_edit <- data.table::fread(paste0(save_location,"/GP_C_ID.txt.gz")) %>%
      dplyr::mutate(primary_care_pop=1) %>%
      dplyr::select(.data$eid,.data$primary_care_pop)
    # combine
    control_populations <- GP_ID_edit %>%
      dplyr::full_join(combined_sex_edit)
    # write
    data.table::fwrite(control_populations,paste0(save_location,"/control_populations"), sep = "\t")
  }
  # DOB ---------------------------------------------------------------------
  if(length(stringr::str_which(available_tab_data,"^52-"))>0 & length(stringr::str_which(available_tab_data,"^34-"))>0){
    # making approximate DOB using year and month of birth set day at 15th
    # year
    YOB <- tab_data %>%
      dplyr::select(.data$eid, tidyselect::matches("^34-")) %>%
      dplyr::rename(YOB=2)
    # month
    MOB <- tab_data %>%
      dplyr::select(.data$eid, tidyselect::matches("^52-")) %>%
      dplyr::rename(MOB=2)
    # DOB
    DOB <- YOB %>%
      dplyr::left_join(MOB) %>%
      dplyr::mutate(day=15,
                    DOB=lubridate::make_date(day=.data$day,month=.data$MOB,year=.data$YOB)) %>%
      dplyr::select(.data$eid,.data$DOB)
    # write
    data.table::fwrite(DOB,paste0(save_location,"/DOB"))
  }
}

# Helpers --------------------------------------------------------------------

# At some point dates in UK Biobank source data have been in DMY format,
# although in latest data dates seem to consistently be in YMD.
parse_date_if_needed <- function(x, orders = c("ymd", "dmy")) {
  if (lubridate::is.Date(x)) {
    # In case input is data.table::IDate need to convert to plain Date
    # in order to placate vctrs type casting rules.
    lubridate::as_date(x)
  } else if (lubridate::is.POSIXt(x)) {
    lubridate::as_date(x)
  } else if (is.character(x)) {
    lubridate::as_date(lubridate::parse_date_time(x, orders))
  } else {
    stop("Don't know how to parse <", class(x)[1], "> to date.")
  }
}

