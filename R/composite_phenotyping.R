#' Makes composite phenotypes guided by composite phenotype map.
#'
#' @param a PheWAS_IDs
#' @param x instructions passed from the composite phenotype map
#' @param phenotypes all available phenotypes generated by the previous functions.
#' @param curated_pheno_list composite phenotypes that have been made starts with none
#' @return A list of data frames of composite phenotypes
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data

curated_phenotype_creation <- function(a,x,phenotypes,curated_pheno_list) {

  pheno_name <- a
  message(pheno_name)
  all_concepts <- x %>%
    dplyr::select(.data$PheWAS_ID,tidyselect::starts_with("C_")) %>%
    tidyr::pivot_longer(tidyselect::starts_with("C_"), names_to = "Concepts", values_drop_na = T) %>%
    dplyr::pull(.data$value)

  all_concepts_edit <- unlist(lapply(seq(1:length(all_concepts)),function(x) unlist(strsplit(all_concepts[[x]],","))))

  (!all_concepts_edit %in% c(names(phenotypes),names(curated_pheno_list)))

  if(!all(all_concepts_edit %in% c(names(phenotypes),names(curated_pheno_list)))) {

    return()

  } else {

    case_input <- x %>%
      dplyr::filter(.data$`Case/control`=="case") %>%
      dplyr::group_split(.data$Case_N)

    control_input <- x %>%
      dplyr::filter(.data$`Case/control`=="control") %>%
      dplyr::group_split(.data$Control_N)


    curated_cases <- mapply(per_line_edit,
                            case_input,
                            MoreArgs = list(phenotypes=phenotypes),
                            SIMPLIFY = F) %>%
      purrr::reduce(dplyr::bind_rows) %>%
      dplyr::group_by(.data$eid) %>%
      dplyr::summarise(any_code=sum(.data$any_code),
                       earliest_date=min(.data$earliest_date))

    if(length(control_input)>0) {

      range_ID <- unique(x$range_ID)

      if(range_ID %in% names(phenotypes)) {

      range_ID_full <- phenotypes[[range_ID]] %>%
        dplyr::pull(.data$eid)
      } else {
        range_ID_full <-c()
      }

      curated_controls <- mapply(per_line_edit,
                                 control_input,
                                 MoreArgs = list(phenotypes=phenotypes),
                                 SIMPLIFY = F) %>%
        purrr::reduce(dplyr::bind_rows) %>%
        dplyr::distinct(.data$eid, .keep_all = T) %>%
        dplyr::mutate(any_code=0) %>%
        dplyr::filter(!.data$eid %in% curated_cases$eid,
                      !.data$eid %in% range_ID_full)

      completed_phenotype <- curated_cases %>%
        dplyr::mutate(earliest_date=as.Date(.data$earliest_date)) %>%
        dplyr::bind_rows(curated_controls)

    } else {

      completed_phenotype <- curated_cases

    }
    return(completed_phenotype)
  }
}

#' Combines concepts for use in creating composite phenotypes
#'
#' @param x Concepts to extract
#' @param y N_codes used to extract concepts
#' @param phenotypes all available phenotypes generated by the previous functions.
#' @return A list of extracted concepts filtered for N_codes
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data
combining_codes <- function(x,y,phenotypes) {
  if(is.character(phenotypes[[x]]$earliest_date)) {
    concept_extracted <- phenotypes[[x]] %>%
      dplyr::rename(any_code=2) %>%
      dplyr::filter(.data$any_code >= y) %>%
      dplyr::mutate(earliest_date=lubridate::ymd(.data$earliest_date))
  } else {
    concept_extracted <- phenotypes[[x]] %>%
      dplyr::rename(any_code=2) %>%
      dplyr::filter(.data$any_code >= y) %>%
      dplyr::mutate(earliest_date=as.Date(.data$earliest_date))
  }
  return(concept_extracted)
}

#' Combines dataframes according to Boolean logical AND.
#'
#' @param a Left hand side of the argument, i.e. a AND b
#' @param b Right hand side of the argument, i.e. a AND b
#' @param c lower limit of the gap between dates of the first code of a and b
#' @param d upper limit of the gap between dates of the first code of a and b
#' @return A dataframe of filtered ID's based on date relationship and AND Boolean logic.
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data
AND <- function(a,b,c,d) {
  case <- a %>%
    dplyr::inner_join(b, by = "eid") %>%
    dplyr::mutate(earliest_date.x = lubridate::ymd(.data$earliest_date.x), earliest_date.y=lubridate::ymd(.data$earliest_date.y),
                  gap = .data$earliest_date.y-.data$earliest_date.x,
                  earliest_date=.data$earliest_date.x,
                  any_code=.data$any_code.x+.data$any_code.y) %>%
    dplyr::filter(dplyr::between(.data$gap,c,d)) %>%
    dplyr::select(.data$eid,.data$any_code,.data$earliest_date)
}

#' Combines dataframes according to Boolean logical NOT.
#'
#' @param a Left hand side of the argument, i.e. a NOT b
#' @param b Right hand side of the argument, i.e. a NOT b
#' @param c lower limit of the gap between dates of the first code of a and b
#' @param d upper limit of the gap between dates of the first code of a and b
#' @return A dataframe of filtered ID's based on date relationship and AND Boolean logic.
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data
NOT <- function(a,b,c,d)  {
  exclusions <- a %>%
    dplyr::inner_join(b, by = "eid") %>%
    dplyr::mutate(earliest_date.x = lubridate::ymd(.data$earliest_date.x), earliest_date.y=lubridate::ymd(.data$earliest_date.y),
                  gap = .data$earliest_date.y-.data$earliest_date.x,
                  earliest_date=.data$earliest_date.x,
                  any_code=.data$any_code.x+.data$any_code.y) %>%
    dplyr::filter(dplyr::between(.data$gap,c,d)) %>%
    dplyr::pull(.data$eid)

  case <- a %>%
    dplyr::filter(!.data$eid %in% exclusions)

}

#' Parses the instruction from composite phenotype map into commands interpretable in R.
#'
#' @param x Instruction for creation of composite phenotypes.
#' @param phenotypes all available phenotypes generated by the previous functions.
#' @return A data frame per PheWAS-ID for composite phenotypes.
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data

per_line_edit <- function(x,phenotypes) {

  . <- NULL
  phenotypes <- phenotypes

  #removing the fixed cols and then removing cols with NA from the columns requiring conversion
  fixed_details <- x %>%
    dplyr::select(.data$PheWAS_ID,.data$phenotype,.data$broad_category,.data$range_ID,.data$`Case/control`,.data$Control_N,.data$Case_N)

  conversion_columns <- x %>%
    dplyr::select(!tidyselect::any_of(colnames(fixed_details))) %>%
    dplyr::select_if(~ !any(is.na(.)))

  all_concepts <- conversion_columns %>%
    dplyr::select(tidyselect::starts_with("C_"))

  # round about way of getting the Number of concepts in each line
  N_concepts <- lapply(colnames(conversion_columns), function(x) stringr::str_count(x,"C_")) %>%
    purrr::reduce(c) %>%
    sum(.)
  # and number of operators
  N_operators <- lapply(colnames(conversion_columns), function(x) stringr::str_count(x,"O_")) %>%
    purrr::reduce(c) %>%
    sum(.)

  # input into function
  concept_seq <- seq(1:N_concepts)
  operator_seq <- seq(1:N_operators)

  if(N_operators==0) {

    C_N <- paste0("C_",1)
    N_N <- paste0("N_",1)
    G_Na <- paste0("G_",1,"a")
    G_Nb <- paste0("G_",1,"b")
    CBB <- paste0("CBB_",1)
    CBA <- paste0("CBA_",1)

    if((x %>% dplyr::select(!!G_Na))[[1]]=="minf"){
      gap_a_convert <- as.difftime(-Inf, units="days") } else {
        gap_a_convert <- as.numeric((x %>% dplyr::select(!!G_Na))[[1]])
      }

    if((x %>% dplyr::select(!!G_Nb))[[1]]=="minf"){
      gap_b_convert <- as.difftime(Inf, units="days") } else {
        gap_b_convert <- as.numeric((x %>% dplyr::select(!!G_Nb))[[1]])
      }

    fully_combined <- list(list(concept_file=list((x %>% dplyr::select(!!C_N))[[1]]),
                                n_codes=list((x %>% dplyr::select(!!N_N))[[1]]),
                                gap_a=list(gap_a_convert),
                                gap_b=list(gap_b_convert)))

    names(fully_combined) <- "Concept_1"

  } else {

    # making lists out of predictable inputs that are consecutively numbered
    concept_conversion <- mapply(concept_list_conversion,concept_seq,MoreArgs = list(y=x),SIMPLIFY = F)
    operator_conversion <- mapply(operator_list_conversion,operator_seq,MoreArgs = list(y=x),SIMPLIFY = F)

    # combines the lists again predictable to make it work remove the last concept
    concept_seq_edit <- seq(1:(N_concepts-1))

    fully_combined <- mapply(making_case_lists,
                             concept_seq_edit,
                             operator_seq,
                             MoreArgs = list(concept_conversion=concept_conversion,operator_conversion=operator_conversion),
                             SIMPLIFY = F) %>%
      purrr::reduce(append) %>%
      append(.,list(concept_conversion[[N_concepts]]))

    combined_names <- unlist(mapply(making_case_names,concept_seq_edit,operator_seq, SIMPLIFY = F)) %>%
      c(paste0("Concept_",N_concepts))

    names(fully_combined) <- combined_names

  }

  # adding the brackets to the correct position in the right list hierarchy
  combined_correct_order <- list()

  for(i in 1:length(fully_combined)) {
    if(stringr::str_detect(names(fully_combined[i]),"Concept")) {
      list_concept_x <- list(fully_combined[[i]])
      names(list_concept_x) <- names(fully_combined)[i]
      concept_x <- fully_combined[[i]]

      if(length(concept_x)>4) {
        bracket_index_concept <- which(names(concept_x)=="bracket")
        concept_list <- list(concept_x[-bracket_index_concept])
        names(concept_list) <- names(fully_combined)[i]

        bracket_string <- unlist(concept_x$bracket)
        bracket_names <- rep("bracket",nchar(bracket_string))
        split_brackets <- as.list(strsplit(bracket_string,"")[[1]])
        names(split_brackets) <- bracket_names

        if(bracket_index_concept==5) {
          combined_correct_order <- append(combined_correct_order,concept_list)
          for(i in 1:length(split_brackets)) {
            combined_correct_order <- append(combined_correct_order,list(bracket=split_brackets[i]))
          }
        } else {
          for(i in 1:length(split_brackets)) {
            combined_correct_order <- append(combined_correct_order,list(bracket=split_brackets[i]))
          }
          combined_correct_order <- append(combined_correct_order,concept_list)
        }
      } else {
        combined_correct_order <- append(combined_correct_order,list_concept_x)
      }

    } else {

      list_operator_x <- list(fully_combined[[i]])
      names(list_operator_x) <- names(fully_combined)[i]
      operator_x <- fully_combined[[i]]

      if(length(operator_x)>1) {
        bracket_index_Boolean <- which(names(operator_x)=="bracket")
        operator_list <- list(operator_x[-bracket_index_Boolean])
        names(operator_list) <- names(fully_combined)[i]

        bracket_string <- unlist(operator_x$bracket)
        bracket_names <- rep("bracket",nchar(bracket_string))
        split_brackets <- as.list(strsplit(bracket_string,"")[[1]])
        names(split_brackets) <- bracket_names

        if(bracket_index_Boolean==2) {
          combined_correct_order <- append(combined_correct_order,operator_list)
          for(i in 1:length(split_brackets)) {
            combined_correct_order <- append(combined_correct_order,list(bracket=split_brackets[i]))
          }
        } else {
          for(i in 1:length(split_brackets)) {
            combined_correct_order <- append(combined_correct_order,list(bracket=split_brackets[i]))
          }
          combined_correct_order <- append(combined_correct_order,operator_list)
        }
      } else {
        combined_correct_order <- append(combined_correct_order,list_operator_x)
      }

    }

  }

  # Now the formula is in the correct order including the bracket terms we can evaluate the formula in the correct order using the function below.
  # works by filling and clearing the list items below as the formula is evaluated left to right.
  LHS_list <- list()
  Boolean_list <- list()
  RHS_list <- list()
  start_bracket_list <- list()
  end_bracket_list <- list()

  for(i in 1:length(combined_correct_order)) {

    if(stringr::str_detect(names(combined_correct_order)[[i]], "Concept")) {

      concept_x <- combined_correct_order[[i]]

      if(length(LHS_list)==0) {
        LHS_list <- append(LHS_list,list(concept=concept_x))

        LHS_concept_codes <- unlist(strsplit(LHS_list[[i]]$concept_file[[1]],","))
        LHS_N_codes <- as.numeric(unlist(strsplit(LHS_list[[i]]$n_codes[[1]],",")))
        evaluated_statement <- mapply(combining_codes,
                                      LHS_concept_codes,
                                      LHS_N_codes,
                                      MoreArgs = list(phenotypes=phenotypes),
                                      SIMPLIFY = F) %>%
          purrr::reduce(dplyr::bind_rows) %>%
          dplyr::group_by(.data$eid) %>%
          dplyr::summarise(any_code=sum(.data$any_code),
                           earliest_date=min(.data$earliest_date))

      } else if(length(start_bracket_list)==length(end_bracket_list)) {
        RHS_list <- append(RHS_list,list(concept=concept_x))
      } else if(length(start_bracket_list)>length(end_bracket_list)) {

        if(length(LHS_list)>=(length(start_bracket_list)-length(end_bracket_list)+1)) {
          RHS_list <- append(RHS_list,list(concept=concept_x))
        } else {
          LHS_list <- append(LHS_list,list(concept=concept_x))
        }

      }

    } else if(stringr::str_detect(names(combined_correct_order)[[i]], "Boolean")) {

      Boolean_x <- combined_correct_order[[i]]

      Boolean_list <- append(Boolean_list,list(Boolean_x))


    }  else if(stringr::str_detect(names(combined_correct_order)[[i]], "bracket")) {

      bracket_x <- combined_correct_order[[i]]

      bracket_extracted <- unlist(bracket_x)

      if(bracket_extracted=="(") {

        start_bracket_list <- append(start_bracket_list,list(bracket_x))

      } else if(bracket_extracted==")") {
        end_bracket_list <- append(end_bracket_list,list(bracket_x))
      }

    }

    if(length(LHS_list)>=1 & length(RHS_list)>=1 & length(Boolean_list)>=1){

      index_LHS <- length(LHS_list)
      index_RHS <- length(RHS_list)
      index_Boolean <- length(Boolean_list)

      if(names(LHS_list[index_LHS])=="concept") {

        LHS_N_codes <- as.numeric(unlist(strsplit(LHS_list[[index_LHS]]$n_codes[[1]],",")))
        LHS_concept_codes <- unlist(strsplit(LHS_list[[index_LHS]]$concept_file[[1]],","))
        LHS_concept <- mapply(combining_codes,
                              LHS_concept_codes,
                              LHS_N_codes,
                              MoreArgs = list(phenotypes=phenotypes),
                              SIMPLIFY = F) %>%
          purrr::reduce(dplyr::bind_rows) %>%
          dplyr::group_by(.data$eid) %>%
          dplyr::summarise(any_code=sum(.data$any_code),
                           earliest_date=min(.data$earliest_date))

      } else if(names(LHS_list[index_LHS])=="evaluated_statement") {

        LHS_N_codes <- 1
        LHS_concept <- LHS_list[[index_LHS]]$concept_file
      }

      if(names(RHS_list[index_RHS])=="concept") {

        RHS_N_codes <- as.numeric(unlist(strsplit(RHS_list[[index_RHS]]$n_codes[[1]],",")))
        gap_a <- unlist(RHS_list[[index_RHS]]$gap_a[[1]],",") %>% as.difftime(units="days")
        gap_b <- unlist(RHS_list[[index_RHS]]$gap_b[[1]],",") %>% as.difftime(units="days")
        RHS_concept_codes <- unlist(strsplit(RHS_list[[index_RHS]]$concept_file[[1]],","))
        RHS_concept <- mapply(combining_codes,
                              RHS_concept_codes,
                              RHS_N_codes,
                              MoreArgs = list(phenotypes=phenotypes),
                              SIMPLIFY = F) %>%
          purrr::reduce(dplyr::bind_rows) %>%
          dplyr::group_by(.data$eid) %>%
          dplyr::summarise(any_code=sum(.data$any_code),
                           earliest_date=min(.data$earliest_date))

      } else if(names(RHS_list[index_RHS])=="evaluated_statement") {

        RHS_N_codes <- 1
        gap_a <- as.difftime(-Inf, units="days")
        gap_b <- as.difftime(Inf, units="days")
        RHS_concept <- RHS_list[[index_RHS]]$concept_file

      }

      Boolean <- unlist(strsplit(Boolean_list[[index_Boolean]]$Boolean[[1]],","))

      if(Boolean=="AND") {
        evaluated_statement <- mapply(AND,list(LHS_concept),list(RHS_concept),list(gap_a),list(gap_b),SIMPLIFY = F)[[1]]

      } else if(Boolean=="NOT") {
        evaluated_statement <- mapply(NOT,list(LHS_concept),list(RHS_concept),list(gap_a),list(gap_b),SIMPLIFY = F)[[1]]

      }

      evaluated_list <- list(concept_file=evaluated_statement,n_codes=list("1"),gap_a=list("-Inf"),gap_b=list("Inf"))

      LHS_list <- LHS_list[-index_LHS]
      Boolean_list <- Boolean_list[-index_Boolean]
      RHS_list <- RHS_list[-index_RHS]

      if(length(start_bracket_list)-length(end_bracket_list)==0) {
        LHS_list <- append(LHS_list,list(evaluated_statement=evaluated_list))

      } else if(length(start_bracket_list)==length(LHS_list) & stringr::str_detect(names(combined_correct_order)[[i+1]], "bracket")) {
        RHS_list <- append(RHS_list,list(evaluated_statement=evaluated_list))

      } else {
        LHS_list <- append(LHS_list,list(evaluated_statement=evaluated_list))
      }
    }

  }
  return(evaluated_statement)
}

#' Helper function for per_line edit
#'
#' @param x A sequence of values representing part of the composite phenotype map.
#' @param y the instructions for the composite phenotype
#' @return A list which can be edited to create composite phenotypes.
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data
concept_list_conversion <- function(x,y) {

  C_N <- paste0("C_",x)
  N_N <- paste0("N_",x)
  G_Na <- paste0("G_",x,"a")
  G_Nb <- paste0("G_",x,"b")
  CBB <- paste0("CBB_",x)
  CBA <- paste0("CBA_",x)

  if((y %>% dplyr::select(!!G_Na))[[1]]=="minf"){
    gap_a_convert <- as.difftime(-Inf, units="days") } else {
      gap_a_convert <- as.numeric((y %>% dplyr::select(!!G_Na))[[1]])
    }

  if((y %>% dplyr::select(!!G_Nb))[[1]]=="minf"){
    gap_b_convert <- as.difftime(Inf, units="days") } else {
      gap_b_convert <- as.numeric((y %>% dplyr::select(!!G_Nb))[[1]])
    }

  concept <- list(concept_file=list((y %>% dplyr::select(!!C_N))[[1]]),
                  n_codes=list((y %>% dplyr::select(!!N_N))[[1]]),
                  gap_a=list(gap_a_convert),
                  gap_b=list(gap_b_convert))

  if((y %>% dplyr::select(!!CBB))[[1]]=="NB" & (y %>% dplyr::select(!!CBA))[[1]]=="NB") {
    return(concept)
  } else if ((y %>% dplyr::select(!!CBB))[[1]]=="NB" & (y %>% dplyr::select(!!CBA))[[1]]!="NB") {
    bracket <- c(list(concept_file=list((y %>% dplyr::select(!!C_N))[[1]]),
                      n_codes=list((y %>% dplyr::select(!!N_N))[[1]]),
                      gap_a=list(gap_a_convert),
                      gap_b=list(gap_b_convert)),
                 list(bracket=list((y %>% dplyr::select(!!CBA))[[1]])))
    return(bracket)
  } else if ((y %>% dplyr::select(!!CBB))[[1]]!="NB" & (y %>% dplyr::select(!!CBA))[[1]]=="NB") {
    bracket <- list(list(bracket=list((y %>% dplyr::select(!!CBA))[[1]])),
                    list(concept_file=list((y %>% dplyr::select(!!C_N))[[1]]),
                         n_codes=list((y %>% dplyr::select(!!N_N))[[1]]),
                         gap_a=list(gap_a_convert),
                         gap_b=list(gap_b_convert)))
    return(bracket)
  }
}

#' Helper function for per_line edit
#'
#' @param x A sequence of values representing part of the composite phenotype map.
#' @param y the instructions for the composite phenotype
#' @return A list which can be edited to create composite phenotypes.
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data
operator_list_conversion <-  function(x,y) {
  O_N <- paste0("O_",x)
  OBB <- paste0("OBB_",x)
  OBA <- paste0("OBA_",x)
  operator <- list(Boolean=list((y %>% dplyr::select(!!O_N))[[1]]))

  if((y %>% dplyr::select(!!OBB))[[1]]=="NB" & (y %>% dplyr::select(!!OBA))[[1]]=="NB") {
    return(operator)
  } else if ((y %>% dplyr::select(!!OBB))[[1]]=="NB" & (y %>% dplyr::select(!!OBA))[[1]]!="NB") {
    bracket <- list(Boolean=list((y %>% dplyr::select(!!O_N))[[1]]),
                    bracket=list((y %>% dplyr::select(!!OBA))[[1]]))
    return(bracket)
  } else if ((y %>% dplyr::select(!!OBB))[[1]]!="NB" & (y %>% dplyr::select(!!OBA))[[1]]=="NB") {
    bracket <- list(bracket=list((y %>% dplyr::select(!!OBB))[[1]]),
                    Boolean=list((y %>% dplyr::select(!!O_N))[[1]]))
    return(bracket)
  }
}
#' Helper function for per_line edit
#'
#' @param x PheWAS_ID
#' @param y primary care code list
#' @param concept_conversion output of concept_list_conversion
#' @param operator_conversion output of operator_list_conversion
#' @return A list combining concept_conversion and operator_conversion files for per_line function
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data
making_case_lists <- function(x,y,concept_conversion,operator_conversion) {
  part_case <- list(concept_conversion[[x]],operator_conversion[[y]])
  return(part_case)
}
#' Helper function for per_line edit
#'
#' @param x list of numbers, labelling numbered concepts.
#' @param y list of numbers, labelling numbered Boolean values.
#' @keywords internal
#' @return A vector of case names.
#' @importFrom magrittr %>%
#' @importFrom rlang .data
making_case_names <- function(x,y) {
  part_case <- c(paste0("Concept_",x),paste0("Boolean_",y))
  return(part_case)
}
#' Makes control populations for composite phenotypes
#'
#' @param x a vector of the control population names.
#' @param control_populations control populations datafarame of Ids.
#' @return control populations in phenotype form.
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom rlang .data
making_control_pop_lists <- function(x, control_populations) {
  selected_population <- control_populations %>%
    dplyr::select(.data$eid,{{x}}) %>%
    dplyr::mutate(earliest_date=lubridate::ymd("2000-01-01")) %>%
    tidyr::drop_na() %>%
    dplyr::select(.data$eid,{{x}},.data$earliest_date)
  return(selected_population)
}

#' Makes composite phenotypes from study data guided by composite phenotype map.
#'
#' @param composite_phenotype_map_overide Full path of the composite_phenotype_map file. Provided with the package.
#' @param phenotype_folder Full path of the folder containing the phenotype data created in previous steps.
#' @param phenotype_files Comma separated full file paths of phenotype data created in previous steps.
#' @param N_iterations Number of iterations curated phenotype script is to run through. Default: 5
#' @param phenotype_save_file Full path for the save file for the generated composite phenotypes RDS.
#' @param control_populations Full path of the "control_populations" file containing columns of IDs, column names are used to create lists of IDs. Saved as a list called control_populations.RDS in the folder inputted in the phenotype_folder flag. The lists are used to define some of the composite phenotype control populations as directed by the composite_phenotype_map file. The unedited version of the composite_phenotype_map file uses two populations, all_pop and primary_care_pop, which represent all IDs in the sample and all IDs with available primary care data. To create composite phenotypes, the names of these list must match the composite_phenotype_map file. The 02_data_preparation.R script creates a control_populations file that is used by default.
#' @param update_list Option to run this script as an update to an existing composite_phenotype list object. If specified, this script will use phenotype_save_file to load the existing list and then save over that file upon completion.
#' @param control_pop_save_file Full path for the save file for the generated composite control populations RDS.
#' @return An RDS file containing lists of data-frames each representing a single composite phenotype.
#' @importFrom magrittr %>%
#' @importFrom rlang .data %||%
#' @export
composite_phenotyping <- function(composite_phenotype_map_overide,
                                  phenotype_folder,
                                  phenotype_files,
                                  N_iterations,
                                  phenotype_save_file,
                                  control_populations,
                                  update_list,
                                  control_pop_save_file = NULL
                                  ) {

  save_file_name <- phenotype_save_file

  if(!is.null(phenotype_folder)){
    phenotype_files_vector <- list.files(phenotype_folder,pattern = ".RDS",full.names = T)
  } else if(!is.null(phenotype_files)){
    phenotype_files_vector <- c(unlist(strsplit(phenotype_files,",")))
  }

  # create control populations to be used in defining curated phenotypes
  if(!file.exists(control_populations)){
    rlang::abort(paste0("'control_populations' must be a file"))
  }
  control_populations <-  data.table::fread(control_populations)
  list_names <- colnames(control_populations)[-1]
  population_lists <- mapply(making_control_pop_lists,list_names,
                             MoreArgs = list(control_populations=control_populations),
                             USE.NAMES = T,
                             SIMPLIFY = F)
  # save
  if(!is.null(phenotype_folder)){
    control_pop_save_file <- control_pop_save_file %||% paste0(phenotype_folder,"/","control_populations.RDS")
  } else if(!is.null(phenotype_files)){
    control_pop_save_file <- control_pop_save_file %||% paste0(dirname(c(unlist(strsplit(phenotype_files,",")))[[1]]),"/","control_populations.RDS")
  }
  saveRDS(population_lists,control_pop_save_file)
  phenotype_files_vector <- c(phenotype_files_vector,control_pop_save_file)

  # number of times the function is iterated over
  if(is.na(as.numeric(N_iterations))){
    rlang::abort(paste0("'N_iterations' must be a numeral"))
  }
  iteration_N <- as.numeric(N_iterations)

  # read in the data from RDS files from the phenotypes folder and combine
   phenotypes <- phenotype_files_vector %>%
    purrr::map(readRDS) %>%
    purrr::reduce(c)
  # read in curated_concept maps
  if(is.null(composite_phenotype_map_overide)) {
    curated_phenotype_map <- data.table::fread(system.file("extdata","composite_phenotype_map.csv.gz", package = "DeepPheWAS"), na.strings = "") %>%
      dplyr::mutate(dplyr::across(tidyselect::everything(),as.character))
  } else {
    if(!file.exists(composite_phenotype_map_overide)){
      rlang::abort(paste0("'composite_phenotype_map_overide' must be a file"))
    }
    curated_phenotype_map <- data.table::fread(composite_phenotype_map_overide, na.strings = "") %>%
      dplyr::mutate(dplyr::across(tidyselect::everything(),as.character))
  }
  # option to update existing list
  if(!is.null(update_list)) {
    curated_pheno_list <- readRDS(update_list)
  } else{
    curated_pheno_list <- list()
  }
  # some curated phenotypes require the initial creation of curated concepts or phenotypes before they can be created
  for(i in 1:iteration_N) {
    curated_phenotype_map_edit <- curated_phenotype_map %>%
      dplyr::filter(!.data$PheWAS_ID %in% names(curated_pheno_list),
                    !.data$PheWAS_ID %in% names(phenotypes))
    if(nrow(curated_phenotype_map_edit)>0) {
      # input is groups by PheWAS_ID
      PheWAS_ID_groups <- curated_phenotype_map_edit %>%
        dplyr::group_split(.data$PheWAS_ID)
      names_PheWAS_ID <- lapply(PheWAS_ID_groups, function(x) input_name <- unique(x$PheWAS_ID)) %>%
        purrr::reduce(c)
      # useful message on log files
      message(names_PheWAS_ID)
      message(length(names_PheWAS_ID))
      # function
      curated_phenotypes <- mapply(curated_phenotype_creation,
                                   names_PheWAS_ID,
                                   PheWAS_ID_groups,
                                   MoreArgs = list(phenotypes=phenotypes,curated_pheno_list=curated_pheno_list),
                                   SIMPLIFY = F,
                                   USE.NAMES = T)
      if(length(which(sapply(curated_phenotypes, is.null)))>0){
        curated_phenotypes = curated_phenotypes[-which(sapply(curated_phenotypes, is.null))]
      }
      # updating lists that will be saved
      curated_pheno_list <- append(curated_pheno_list,curated_phenotypes)
      phenotypes <- append(phenotypes,curated_phenotypes)

    } else {}
  }
  # saved as R object
  saveRDS(curated_pheno_list,save_file_name)
}
