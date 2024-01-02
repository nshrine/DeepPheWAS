FROM rocker/r-ver:4.3.1

ENV BGENIX_VERSION=v1.1.4-CentOS6.8-x86_64
ENV PLINK_VERSION=x86_64_20231018
ENV PLINK2_VERSION=avx2_20220514

RUN install2.r --error R.utils bit64 pak && \
	Rscript -e 'pak::pak("nshrine/DeepPheWAS")' 

RUN apt update && apt install wget && \
	wget https://www.chg.ox.ac.uk/~gav/resources/bgen_$BGENIX_VERSION.tgz && \
	tar --strip-components 1 -C /usr/local/bin -zxvf bgen_$BGENIX_VERSION.tgz bgen_$BGENIX_VERSION/bgenix && \
	rm bgen_$BGENIX_VERSION.tgz && \
	wget https://s3.amazonaws.com/plink1-assets/plink_linux_$PLINK_VERSION.zip && \
	unzip -d /usr/local/bin plink_linux_$PLINK_VERSION.zip plink && \
	rm plink_linux_$PLINK_VERSION.zip && \
	wget https://s3.amazonaws.com/plink2-assets/alpha3/plink2_linux_$PLINK2_VERSION.zip && \
	unzip -d /usr/local/bin plink2_linux_$PLINK2_VERSION.zip plink2 && \
	rm plink2_linux_$PLINK2_VERSION.zip
