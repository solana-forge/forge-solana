use tonic_build::configure;

fn main() -> Result<(), std::io::Error> {
    const PROTOC_ENVAR: &str = "PROTOC";
    if std::env::var(PROTOC_ENVAR).is_err() {
        #[cfg(not(windows))]
        std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc());
    }

    let proto_base_path = std::path::PathBuf::from("protos");
    let proto_files = ["pbs.proto", "bundle.proto", "packet.proto"];
    let mut protos = Vec::new();
    for proto_file in &proto_files {
        let proto = proto_base_path.join(proto_file);
        println!("cargo:rerun-if-changed={}", proto.display());
        protos.push(proto);
    }

    configure()
        .build_client(true)
        .build_server(false)
        .compile(&protos, &[proto_base_path])
}
