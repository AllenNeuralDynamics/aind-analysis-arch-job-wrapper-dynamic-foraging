# hash:sha256:ef64ff17fc6ca0c678f82140d5b19c8259e32919a509a4d099642e97efe36d36
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/jupyterlab:3.6.1-miniconda4.12.0-python3.9-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

RUN pip install --no-cache-dir --upgrade \
    pynwb \
    s3fs \
    aind_data_access_api[docDB] \
    aind-dynamic-foraging-models==0.12.1 \
    aind_dynamic_foraging_data_utils

ADD "https://github.com/coder/code-server/releases/download/v4.21.1/code-server-4.21.1-linux-amd64.tar.gz" /.code-server/code-server.tar.gz

RUN cd /.code-server \
    && tar -xvf code-server.tar.gz \
    && rm code-server.tar.gz \
    && ln -s /.code-server/code-server-4.21.1-linux-amd64/bin/code-server  /usr/bin/code-server

COPY postInstall /
RUN /postInstall
