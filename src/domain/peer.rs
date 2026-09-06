use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct NearbyPeer {
    pub id: String,
    pub name: String,
    pub platform: String,
    pub model: String,
    pub address: String,
    #[serde(default)]
    pub addresses: Vec<String>,
    pub port: u16,
    pub public_key: String,
    pub certificate_sha256: String,
    pub paired: bool,
    pub automatic_receive: bool,
    pub last_seen_at_ms: i64,
}

impl NearbyPeer {
    pub fn endpoint(&self) -> crate::Result<std::net::SocketAddr> {
        self.endpoints()?
            .into_iter()
            .next()
            .ok_or_else(|| crate::TransferError::Invalid("invalid peer address".into()))
    }

    pub fn endpoints(&self) -> crate::Result<Vec<std::net::SocketAddr>> {
        let mut endpoints = Vec::new();
        for value in std::iter::once(&self.address).chain(self.addresses.iter()) {
            let Ok(address) = value.parse::<std::net::IpAddr>() else {
                continue;
            };
            let endpoint = std::net::SocketAddr::new(address, self.port);
            if !endpoints.contains(&endpoint) {
                endpoints.push(endpoint);
            }
        }
        if endpoints.is_empty() {
            return Err(crate::TransferError::Invalid("invalid peer address".into()));
        }
        Ok(endpoints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(address: &str, addresses: &[&str]) -> NearbyPeer {
        NearbyPeer {
            id: "peer".into(),
            name: "Peer".into(),
            platform: "test".into(),
            model: "test".into(),
            address: address.into(),
            addresses: addresses.iter().map(|value| (*value).into()).collect(),
            port: 18765,
            public_key: "key".into(),
            certificate_sha256: "digest".into(),
            paired: true,
            automatic_receive: false,
            last_seen_at_ms: 0,
        }
    }

    #[test]
    fn endpoints_keep_all_valid_unique_addresses() {
        let endpoints = peer("10.1.1.91", &["10.1.1.91", "invalid", "10.1.1.249"])
            .endpoints()
            .unwrap();

        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].ip().to_string(), "10.1.1.91");
        assert_eq!(endpoints[1].ip().to_string(), "10.1.1.249");
    }

    #[test]
    fn endpoints_reject_peer_without_any_valid_address() {
        assert!(peer("invalid", &["also-invalid"]).endpoints().is_err());
    }
}
