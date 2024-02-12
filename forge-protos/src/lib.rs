pub mod proto {
    pub mod pbs {
        tonic::include_proto!("pbs");
    }

    pub mod bundle {
        tonic::include_proto!("bundle");
    }

    pub mod packet {
        tonic::include_proto!("packet");
    }
}
