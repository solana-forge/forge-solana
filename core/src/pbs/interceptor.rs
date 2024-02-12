use {
    solana_version::version,
    tonic::{service::Interceptor, Request, Status},
};

pub(crate) struct AuthInterceptor {
    uuid: String,
    version: String,
}

impl AuthInterceptor {
    pub(crate) fn new(uuid: String) -> Self {
        Self {
            uuid,
            version: version!().to_string(),
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.uuid.parse().unwrap());
        request
            .metadata_mut()
            .insert("version", self.version.parse().unwrap());

        Ok(request)
    }
}
