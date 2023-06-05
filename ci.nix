{ nospdk ? false, norust ? false, asan ? false }:
let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays =
      [ (_: _: { inherit sources; }) (import ./nix/overlay.nix { }) ];
  };
in
with pkgs;
let
  nospdk_moth =
    "You have requested environment without SPDK, you should provide it!";
  norust_moth =
    "You have requested environment without RUST, you should provide it!";
  channel = import ./nix/lib/rust.nix { inherit sources; };
  # python environment for test/python
  pytest_inputs = python3.withPackages
    (ps: with ps; [ virtualenv grpcio grpcio-tools asyncssh black ]);
in
mkShell {
  name = "io-engine-dev-shell";
  # fortify does not work with -O0 which is used by spdk when --enable-debug
  hardeningDisable = [ "fortify" ];
  buildInputs = [
    clang_11
    cowsay
    docker
    docker-compose
    e2fsprogs
    etcd
    fio
    gdb
    git
    kubernetes-helm
    libaio
    libbsd
    libnvme
    libpcap
    udev
    liburing
    llvmPackages_11.libclang
    meson
    ninja
    nodejs-16_x
    nvme-cli
    numactl
    openssl
    pkg-config
    pre-commit
    procps
    pytest_inputs
    python3
    utillinux
    xfsprogs
    gnuplot
    libunwind
    autoconf
    automake
    yasm
  ] ++ (if (nospdk) then [ libspdk-dev.buildInputs ] else [ libspdk-dev ])
  ++ pkgs.lib.optional (!norust && asan) channel.asan
  ++ pkgs.lib.optional (!norust && !asan) channel.stable
  ++ pkgs.lib.optional (!norust) channel.nightly;

  RUST_NIGHTLY_PATH = channel.nightly;
  LIBCLANG_PATH = io-engine.LIBCLANG_PATH;
  PROTOC = io-engine.PROTOC;
  PROTOC_INCLUDE = io-engine.PROTOC_INCLUDE;
  SPDK_PATH = if nospdk then null else "${libspdk-dev}";
  FIO_SPDK = if nospdk then null else "${libspdk-dev}/fio/spdk_nvme";
  ETCD_BIN = "${etcd}/bin/etcd";

  ASAN_ENABLE = if asan then "1" else null;
  ASAN_OPTIONS = if asan then "detect_leaks=0" else null;
  RUSTFLAGS = if asan then "-Zsanitizer=address" else null;
  RUST_BACKTRACE = if asan then "1" else null;

  shellHook = ''
    ${pkgs.lib.optionalString (nospdk) "cowsay ${nospdk_moth}"}
    ${pkgs.lib.optionalString (nospdk) "export CFLAGS=-msse4"}
    ${pkgs.lib.optionalString (nospdk) "echo"}
    ${pkgs.lib.optionalString (norust) "cowsay ${norust_moth}"}
    ${pkgs.lib.optionalString (norust) "echo 'Hint: use rustup tool.'"}
    ${pkgs.lib.optionalString (norust) "echo"}

    ${pkgs.lib.optionalString (asan) "export LLVM_SYMBOLIZER_DIR=$(dirname $(realpath $(which llvm-symbolizer)))"}

    ${pkgs.lib.optionalString (asan) "echo 'AddressSanitizer is enabled, forcing nightly rustc.'"}
    ${pkgs.lib.optionalString (asan) "echo '  ASAN_ENABLE         =' $\{ASAN_ENABLE\}"}
    ${pkgs.lib.optionalString (asan) "echo '  ASAN_OPTIONS        =' $\{ASAN_OPTIONS\}"}
    ${pkgs.lib.optionalString (asan) "echo '  RUSTFLAGS           =' $\{RUSTFLAGS\}"}
    ${pkgs.lib.optionalString (asan) "echo '  RUST_BACKTRACE      =' $\{RUST_BACKTRACE\}"}
    ${pkgs.lib.optionalString (asan) "echo '  LLVM_SYMBOLIZER_DIR =' $\{LLVM_SYMBOLIZER_DIR\}"}
    ${pkgs.lib.optionalString (asan) "echo"}

    echo 'Using' $(rustc --version)
    echo

    # SRCDIR is needed by docker-compose files as it requires absolute paths
    export SRCDIR=`pwd`
    pre-commit install
    pre-commit install --hook commit-msg
  '';
}
