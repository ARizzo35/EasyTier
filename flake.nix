{
  description = "A dev shell for EasyTier.";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = inputs@{ flake-utils, nixpkgs, rust-overlay, ... }:
  flake-utils.lib.eachDefaultSystem (system:
  let
    inherit (nixpkgs) lib;
    pkgs = import nixpkgs {
      inherit system;
      overlays = [
        rust-overlay.overlays.default
      ];
    };
  in
  {
    devShells = {
      default = pkgs.mkShell {
        packages = with pkgs; [
          cargo
          clang
          llvmPackages.libclang
          libcxx
          stdenv.cc
        ];

        shellHook = ''
          export LIBCLANG_PATH="${pkgs.llvmPackages.libclang.lib}/lib"
        '';
      };
    };
  });
}
