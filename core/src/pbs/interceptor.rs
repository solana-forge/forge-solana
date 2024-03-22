use {
    solana_sdk::{
        hash::Hash,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
    },
    solana_version::version,
    tonic::{service::Interceptor, Request, Status},
};

pub struct AuthInterceptor {
    uid: String,
    version: String,
    pubkey: Pubkey,
    blockhash: Hash,
    signature: Signature,
}

impl AuthInterceptor {
    pub(crate) fn new(uid: String, keypair: &Keypair, blockhash: Hash) -> Self {
        let pubkey = keypair.pubkey();
        let challenge = format!("{pubkey}-{uid}-{blockhash}");
        let signature = keypair.sign_message(challenge.as_bytes());

        Self {
            uid,
            blockhash,
            pubkey,
            version: version!().to_string(),
            signature,
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("signature", self.signature.to_string().parse().unwrap());
        request
            .metadata_mut()
            .insert("blockhash", self.blockhash.to_string().parse().unwrap());
        request
            .metadata_mut()
            .insert("uid", self.uid.parse().unwrap());
        request
            .metadata_mut()
            .insert("version", self.version.parse().unwrap());
        request
            .metadata_mut()
            .insert("validator", self.pubkey.to_string().parse().unwrap());

        Ok(request)
    }
}
