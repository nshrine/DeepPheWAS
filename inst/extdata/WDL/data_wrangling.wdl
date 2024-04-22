version 1.0

task minimum_data {

	input {
		Array[File]+ files
		File? exclusions
		String save_loc = "."
	}

	Int cores = 16
	String min_data = save_loc + "/minimum_tab_data.gz"

	command <<<
		[ -d ~{save_loc} ] || mkdir ~{save_loc}

		Rscript `Rscript -e 'cat(system.file("extdata/scripts/phenotype_generation","01_minimum_data.R", package = "DeepPheWAS"))'` \
			--data_files ~{sep="," files} \
			~{"--exclusions " + exclusions} \
			--save_loc ~{min_data} \
			--N_cores ~{cores}
	>>>

	output {
		File out = min_data
	}

	runtime {
		cpu: cores
		memory: "120 GB"
	}
}

task data_preparation {

	input {
		String save_loc = "."
		File min_data
		File GPC
		File GPP
		File hesin_diag
		File HESIN
		File hesin_opr
		File death_cause
		File death
		File? king_coef
	}

	command <<<
		[ -d ~{save_loc} ] || mkdir ~{save_loc}

		Rscript `Rscript -e 'cat(system.file("extdata/scripts/phenotype_generation","02_data_preparation.R", package = "DeepPheWAS"))'` \
			--save_location ~{save_loc} \
			--min_data ~{min_data} \
			--GPC ~{GPC} \
			--GPP ~{GPP} \
			--hesin_diag ~{hesin_diag} \
			--HESIN ~{HESIN} \
			--hesin_oper ~{hesin_opr} \
			--death_cause ~{death_cause} \
			--death ~{death} \
			--king_coef ~{king_coef}
	>>>

	output {
		File health_records = "health_records.txt.gz"
		File combined_sex = "combined_sex"
		File? control_exclusions = "control_exclusions"
		File? related_callrate = "related_callrate"
		File GP_P = "GP_P_edit.txt.gz"
		File GP_C = "GP_C_edit.txt.gz"
		File? GP_ID = "GP_C_ID.txt.gz"
		File? control_populations = "control_populations"
		File DOB = "DOB"
		Array[File] out = glob(save_loc + "/*")
	}

	runtime {
		cpu: 1
		memory: "120 GB"
	}
}
