with import <nixpkgs> { };
let
  # python environment for test/python
  pytest_inputs = python3.withPackages
    (ps: with ps; [ virtualenv grpcio grpcio-tools asyncssh black ]);
in
mkShell {
  name = "spdk-build-shell";

  # fortify does not work with -O0 which is used by spdk when --enable-debug
  hardeningDisable = [ "fortify" ];

  buildInputs = [
    clang_11
    binutils
    etcd
    jansson
    libaio
    libbsd
    libiscsi
    libpcap
    libudev
    liburing
    llvmPackages_11.libclang
    meson
    nasm
    nats-server
    ncurses
    ninja
    numactl
    nvme-cli
    openssl
    pkg-config
    pre-commit
    procps
    pytest_inputs
    python3
    (python3.withPackages (ps: with ps; [ pyelftools ]))
    utillinux
  ];

  shellHook = ''
    cat << EOF

This Nix shell environment is intended for making a custom SPDK build to use
with spdk-rs crate:

Configure and build SPDK with:

  cd <your spdk dir>
  git checkout <compatible branch or commit>
  ./configure --enable-debug --target-arch=nehalem --without-shared --without-isal --with-crypto --with-uring --disable-unit-tests --disable-tests --without-fio
  make

Clean up SPDK directory:

  git clean -fdx ./ && git submodule foreach --recursive git clean -xfd && git submodule update

Enter a shell with spdk-rs environment and build spdk-rs crate with SPDK_PATH:

  nix-shell --arg nospdk true
  cd <your spdk-rs crate dir>
  export SPDK_PATH=<your spdk dir>
  cargo build

or using a symlink:

  nix-shell --arg nospdk true
  cd <your spdk-rs crate dir>
  ln -s <your spdk dir> spdk
  cargo build

EOF
  '';
}
