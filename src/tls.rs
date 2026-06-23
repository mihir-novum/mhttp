use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use rustls_pemfile::{certs, private_key};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;

#[derive(thiserror::Error, Debug)]
pub enum TlsConfigError {
    #[error("failed to read certificate file: {0}")]
    CertRead(std::io::Error),
    #[error("failed to read private key file: {0}")]
    KeyRead(std::io::Error),
    #[error("no private key found in key file")]
    NoPrivateKey,
    #[error("TLS config error: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("failed to generate self-signed certificate: {0}")]
    SelfSigned(#[from] rcgen::Error),
}

pub enum TlsConfig {
    /// Dev: generate a self-signed cert at runtime for the given hostnames.
    /// Typical usage: `TlsConfig::SelfSigned { domains: vec!["localhost".into()] }`
    SelfSigned { domains: Vec<String> },

    /// Prod: load cert chain + private key from PEM files on disk.
    /// cert_path: full chain (leaf → intermediates → root)
    /// key_path:  PKCS#8 or PKCS#1 private key
    FromFiles {
        cert_path: String,
        key_path: String,
    },
}

impl TlsConfig {
    pub(crate) fn build(self) -> Result<TlsAcceptor, TlsConfigError> {
        let (certs, key) = match self {
            TlsConfig::SelfSigned { domains } => self_signed_pair(domains)?,
            TlsConfig::FromFiles { cert_path, key_path } => {
                load_from_files(&cert_path, &key_path)?
            }
        };

        let config = ServerConfig::builder()
            .with_no_client_auth()          // no mutual TLS
            .with_single_cert(certs, key)?;

        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

fn self_signed_pair(
    domains: Vec<String>,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsConfigError> {
    use rcgen::{generate_simple_self_signed, CertifiedKey};

    let CertifiedKey { cert, key_pair, .. } = generate_simple_self_signed(domains)?;

    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(key_pair.serialize_der())
        .expect("rcgen always emits a valid PKCS#8 key");

    Ok((vec![cert_der], key_der))
}

// --- from PEM files (prod) --------------------------------------------

fn load_from_files(
    cert_path: &str,
    key_path: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsConfigError> {
    let cert_file = File::open(cert_path).map_err(TlsConfigError::CertRead)?;
    let key_file  = File::open(key_path).map_err(TlsConfigError::KeyRead)?;

    // rustls-pemfile parses the PEM envelopes and returns raw DER bytes
    let certs: Vec<CertificateDer<'static>> = certs(&mut BufReader::new(cert_file))
        .collect::<Result<_, _>>()
        .map_err(TlsConfigError::CertRead)?;

    let key: PrivateKeyDer<'static> = private_key(&mut BufReader::new(key_file))
        .map_err(TlsConfigError::KeyRead)?
        .ok_or(TlsConfigError::NoPrivateKey)?;

    Ok((certs, key))
}