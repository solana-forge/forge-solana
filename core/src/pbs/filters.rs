use {
    crate::pbs::PbsError,
    forge_protos::proto::pbs::SubscriptionFiltersResponse,
    solana_sdk::{pubkey::Pubkey, transaction::SanitizedTransaction},
};

#[derive(Clone, Eq, PartialEq)]
pub struct SubscriptionFilters {
    stream_all: bool,
    account_include: Vec<Pubkey>,
    account_exclude: Vec<Pubkey>,
    account_required: Vec<Pubkey>,
}

impl TryFrom<SubscriptionFiltersResponse> for SubscriptionFilters {
    type Error = PbsError;
    fn try_from(value: SubscriptionFiltersResponse) -> Result<Self, Self::Error> {
        let SubscriptionFiltersResponse {
            stream_all,
            account_include,
            account_exclude,
            account_required,
        } = value;

        Ok(Self {
            stream_all: stream_all.unwrap_or_default(),
            account_include: decode_pubkeys_into_vec(account_include)?,
            account_exclude: decode_pubkeys_into_vec(account_exclude)?,
            account_required: decode_pubkeys_into_vec(account_required)?,
        })
    }
}

fn decode_pubkeys_into_vec(pubkeys: Vec<Vec<u8>>) -> Result<Vec<Pubkey>, PbsError> {
    let mut vec: Vec<_> = pubkeys
        .into_iter()
        .map(Pubkey::try_from)
        .collect::<Result<_, _>>()
        .map_err(|_| PbsError::SubscriptionFiltersError)?;
    vec.sort();
    Ok(vec)
}

impl SubscriptionFilters {
    pub fn is_tx_have_to_be_processed(&self, transaction: &SanitizedTransaction) -> bool {
        if self.stream_all {
            return true;
        }

        let accounts = transaction.message().account_keys();

        if !self.account_include.is_empty()
            && accounts
                .iter()
                .all(|pubkey| self.account_include.binary_search(pubkey).is_err())
        {
            return false;
        }

        if !self.account_exclude.is_empty()
            && accounts
                .iter()
                .any(|pubkey| self.account_exclude.binary_search(pubkey).is_ok())
        {
            return false;
        }

        if !self.account_required.is_empty() {
            let mut other: Vec<&Pubkey> = accounts.iter().collect();

            let is_subset = if self.account_required.len() <= other.len() {
                other.sort();
                self.account_required
                    .iter()
                    .all(|pubkey| other.binary_search(&pubkey).is_ok())
            } else {
                false
            };

            if !is_subset {
                return false;
            }
        }

        true
    }
}
