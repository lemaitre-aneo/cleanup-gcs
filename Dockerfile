FROM registry.access.redhat.com/ubi7/ubi:7.9 AS builder

RUN \
      yum clean all && \
      yum --disableplugin=subscription-manager update -y && \
      rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && \
      yum --disableplugin=subscription-manager clean all

COPY ./CentOS-7-Linux-AppStream.repo \
     /etc/yum.repos.d/CentOS-Base.repo

# Copy public keys for repo GPG check
RUN curl https://raw.githubusercontent.com/sclorg/centos-release-scl/master/centos-release-scl/RPM-GPG-KEY-CentOS-SIG-SCLo \
  > /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-SCLo \
  && curl -L https://www.centos.org/keys/RPM-GPG-KEY-CentOS-7 > /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 \
  # Import public keys for repo GPG check
  && gpg --import \
            /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 \
            /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-SCLo

# Enable yum repos
RUN yum-config-manager --disableplugin=subscription-manager --enable \
                         ubi-server-rhscl-7-rpms \
                         ubi-7-server-optional-rpms

RUN yum --disableplugin=subscription-manager update -y && \
    yum --disableplugin=subscription-manager \
    install -y  bash \
      wget \
      binutils \
      yum-utils \
      gcc gcc-c++ make \
      curl \
      jq \
      libffi-devel \
      gnome-keyring \
      procps \
      zlib-devel \
      bzip2 \
      wget \
      bzip2-devel \
      dbus-x11 \
      git \
      rh-python38-python-devel \
      centos-release-scl \
      devtoolset-10 openssl openssl-devel \ 
      && yum --disableplugin=subscription-manager clean all
RUN curl https://sh.rustup.rs -sSf > rustup.sh
RUN chmod +x ./rustup.sh && ./rustup.sh -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo init --name cleanup-gcs
COPY Cargo.* .
RUN cargo build -r
COPY src src
RUN touch src/main.rs && cargo build -r && mv target/release/cleanup-gcs .

FROM centos:7 AS final
COPY --from=builder cleanup-gcs .
