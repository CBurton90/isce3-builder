FROM nvidia/cuda:12.6.3-devel-ubuntu24.04 as builder

ENV LANG en_US.UTF-8
ENV TZ Pacific/Auckland
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
	apt-get install -y \
	curl \
	unzip \
	cmake \
	ninja-build \
	git \
	g++ \
	python3 \
	python3-dev \
	python3-pip \
	python3-pytest \
	python3-numpy \
	python3-scipy \
	python3-h5py \
	python3-gdal \
	python3-shapely \
	libeigen3-dev \
	libfftw3-dev \
	libgdal-dev \
	libhdf5-dev \
	libgsl-dev \
	pybind11-dev \
	python3-pybind11 \
	gdal-bin

RUN pip3 install backoff pyaps3 pysolid raider snaphu yamale ruamel.yaml --break-system-packages

WORKDIR /opt

RUN curl -fL https://github.com/isce-framework/isce3/archive/refs/tags/v0.25.7.tar.gz -o isce3.tar.gz \
	&& tar -xzf isce3.tar.gz \
	&& rm isce3.tar.gz

RUN curl https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip \
	&& unzip awscliv2.zip

WORKDIR /opt/isce3-0.25.7

RUN set -ex \
	&& mkdir build && cd build \
	&& export CUDAHOSTCXX=$CXX \
	&& export CUDACXX=/usr/local/cuda/bin/nvcc \
	&& cmake .. -GNinja -DWITH_CUDA=OFF -DCMAKE_INSTALL_PREFIX=./install \
	&& ninja install

FROM builder as tester
WORKDIR /opt/isce3-0.25.7/build
RUN ctest --output-on-failure

FROM nvidia/cuda:12.6.3-devel-ubuntu24.04 as final

RUN apt-get update && apt-get install -y \
	python3 \
	python3-pip \
	python3-numpy \
	python3-scipy \
	python3-h5py \
	python3-gdal \
	python3-shapely \
	libfftw3-dev \
	libgdal-dev \
	libhdf5-dev \
	gdal-bin

RUN pip3 install backoff pyaps3 pysolid raider snaphu yamale ruamel.yaml --break-system-packages

COPY --from=builder /opt/isce3-0.25.7/build/install/lib /opt/isce3/lib
COPY --from=builder /opt/isce3-0.25.7/build/install/packages /opt/isce3/packages
COPY --from=builder /opt/isce3-0.25.7/build/install/bin /opt/isce3/bin
COPY --from=builder /opt/isce3-0.25.7/share/nisar/defaults /opt/isce3/share/nisar/defaults
COPY --from=builder /opt/aws /opt/aws

ENV LD_LIBRARY_PATH=/opt/isce3/lib
ENV PYTHONPATH=/opt/isce3/packages

RUN python3 -c 'import isce3; print(isce3.__version__)'
RUN sudo ./aws/install

COPY download_dem.sh /opt/download_COP_dem.sh
RUN chmod +x /opt/download_COP_dem.sh
