version 1.0

workflow association_testing {

	input {
		File? phewas_manifest
		File snp_list
		String analysis_name
		File? covariates
		File? phenotype_inclusion_file
	}

	call extracting_snps {
		input:
			snp_list = snp_list,
			variant_save_name = analysis_name
	}

	call PLINK_association_testing {
		input:
			pgen = extracting_snps.pgen,
			psam = extracting_snps.psam,
			pvar = extracting_snps.pvar,
			covariates = covariates,
			analysis_name = analysis_name,
			phewas_manifest = phewas_manifest,
			phenotype_inclusion_file = phenotype_inclusion_file
	}
	
	call tables_graphs {
		input:
			results_file = PLINK_association_testing.results,
			analysis_name = analysis_name,
			snp_list = snp_list,
			phewas_manifest = phewas_manifest
	}

	output {
		Array[File?] prep_pheno_out = if prepare_phenotypes then [ phenotype_preparation.phenotype, phenotype_preparation.stats ] else []
		File assoc_out = PLINK_association_testing.results
		Array[File?] tables_graphs_out = tables_graphs.out
	}
}

task phenotype_preparation {

	input {
		Array[File]+ phenotype_files
		File groupings
		String phenotype_filtered_save_name
		Boolean relate_remove = true
		File? kinship_file
		Boolean IVNT = true
		String stats_save = "IVNT_RR_N_filtered_neuro_PanUKB_ancestry_stats"
		File? phewas_manifest
	}

	command <<<
		SCRIPT=`Rscript -e 'cat(system.file("extdata/scripts/association_testing","01_phenotype_preparation.R", package = "DeepPheWAS"))'` && \
		echo $SCRIPT && \
		Rscript $SCRIPT \ 
			--phenotype_filtered_save_name ~{phenotype_filtered_save_name} \
			--phenotype_files ~{sep="," phenotype_files} \
			~{"--groupings " + groupings } \
			~{true="--relate_remove" false="" relate_remove} \
			~{"--kinship_file " + kinship_file} \
			~{true="--IVNT" false="" IVNT} \
			--stats_save ~{stats_save} \
			~{"--PheWAS_manifest_overide " + phewas_manifest} \
	>>>

	output {
		File phenotype = phenotype_filtered_save_name
		File stats = stats_save
	}

	runtime {
		cpu: 1
		memory: "200 GB"
	}
}

task extracting_snps {

	input {
		Array[File]+ bgens
		Array[File]+ bgis
		Array[File]+ sample_files
		File snp_list
		String variant_save_name
	}

	parameter_meta {
		bgens : "stream"
  }

	command <<<
		echo ~{sep="," bgis} &&
		awk -F ',' 'BEGIN { OFS="," } NR == 1 { print "chromosome", "genetic_file_location", "psam_fam_sample_file_location"; next } !($1 in chr) { print $1, "chr"$1, "chr"$1".sample"; chr[$1] }' ~{snp_list} > genetic_file_guide.csv &&
		awk -F ',' 'NR > 1 { print $1, $2, $3 }' genetic_file_guide.csv | while read i b s; do
			cp `tr ',' '\n' <<< "~{sep=',' bgens}" | grep _c${i}_` ${b}.bgen
			cp `tr ',' '\n' <<< "~{sep=',' bgis}" | grep _c${i}_` ${b}.bgen.bgi
			cp `tr ',' '\n' <<< "~{sep=',' sample_files}" | grep _c${i}_` ${b}.sample
		done &&
		Rscript `Rscript -e 'cat(system.file("extdata/scripts/association_testing","02_extracting_snps.R", package = "DeepPheWAS"))'` \
		--SNP_list ~{snp_list} \
		--genetic_file_guide genetic_file_guide.csv \
		--analysis_folder . \
		--bgen_input \
		--ref_bgen ref-first \
		--variant_save_name ~{variant_save_name}
	>>>

	output {
		File pgen = variant_save_name + ".pgen"
		File psam = variant_save_name + ".psam"
		File pvar = variant_save_name + ".pvar"
	}

	runtime {
		cpu: 1
		memory: "24 GB"
		disks: 300
	}
}	

task PLINK_association_testing {

	input {
		Array[File]+ phenotypes
		File pgen
		File psam
		File pvar
		File? covariates
		String analysis_name
		File? phewas_manifest
		File? phenotype_inclusion_file
	}

	Int cores = 8

	command <<<
		echo ~{psam} ~{pvar} &&
		`Rscript -e 'cat(system.file("extdata/scripts/association_testing","03a_PLINK_association_testing.R", package = "DeepPheWAS"))'` \
		--analysis_folder ./ \
		--phenotype_files ~{sep="," phenotypes} \
		--variants_for_association ~{pgen} \
		~{"--PheWAS_manifest_overide " + phewas_manifest} \
		~{"--covariate " + covariates} \
		--analysis_name ~{analysis_name} \
		--plink_exe "plink2 --threads ~{cores}" \
		--save_plink_tables \
		~{"--phenotype_inclusion_file " + phenotype_inclusion_file}
	>>>

	output {
		File results = "association_results/" + analysis_name + "_association_results_list.gz"
	}

	runtime {
		cpu: cores
		memory: "200 GB"
	}
}

task tables_graphs {

	input {
		File results_file
		String analysis_name
		File snp_list
		File? phewas_manifest
	}

	command <<<
		`Rscript -e 'cat(system.file("extdata/scripts/association_testing","05_tables_graphs.R", package = "DeepPheWAS"))'` \
			--results_file ~{results_file} \
			--analysis_name ~{analysis_name} \
			--plink_results \
			--SNP_list ~{snp_list} \
			~{"--PheWAS_manifest_overide " + phewas_manifest} \
			--per_group_name_graph \
			--save_table_per_snp \
			--save_table_per_group_name \
			--sex_split \
			--save_folder "./"
	>>>

	output {
		Array[File]+ out = glob("*")
	}

	runtime {
		cores: 1
		memory: "64 GB"
	}
}
