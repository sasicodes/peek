use std::env;

use clap::Parser;
use peek_client::TunnelClient;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        eprintln!("run `peek --help` for usage");
        std::process::exit(1);
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "peek",
    version,
    about = "Create a public URL for a local server through your own peek relay."
)]
struct Cli {
    #[arg(
        value_name = "LOCAL_ADDRESS",
        help = "Local address, like localhost:3000 or 3000"
    )]
    local: String,

    #[arg(
        long,
        value_name = "URL",
        help = "Full WebSocket URL, like wss://example.com/tunnel"
    )]
    server: Option<String>,

    #[arg(
        long,
        value_name = "DOMAIN",
        help = "Hosted peek domain, like example.com"
    )]
    domain: Option<String>,

    #[arg(
        long,
        value_name = "TOKEN",
        help = "Server token used to create a tunnel"
    )]
    token: Option<String>,

    #[arg(
        long,
        value_name = "PASSWORD",
        help = "Require this password for visitors"
    )]
    password: Option<String>,

    #[arg(long, value_name = "NAME", help = "Public URL name, like myapp")]
    subdomain: Option<String>,
}

async fn run() -> Result<(), String> {
    let cli = Cli::parse();
    let port = parse_local_port(&cli.local)?;
    let local_url = local_url(&cli.local, port);

    let server_url = cli
        .server
        .or_else(|| env::var("PEEK_SERVER").ok())
        .or_else(|| {
            cli.domain
                .or_else(|| env::var("PEEK_DOMAIN").ok())
                .map(|domain| format!("wss://{domain}/tunnel"))
        })
        .ok_or("missing relay: pass --domain, --server, or set PEEK_DOMAIN")?;
    let token = cli
        .token
        .or_else(|| env::var("PEEK_AUTH_TOKEN").ok())
        .or_else(|| env::var("PEEK_TOKEN").ok());
    let password = cli.password.or_else(|| env::var("PEEK_PASSWORD").ok());
    let password_enabled = password
        .as_ref()
        .is_some_and(|password| !password.is_empty());

    let mut client = TunnelClient::new(&server_url).map_err(|error| error.to_string())?;
    if let Some(token) = token {
        client = client.with_token(token);
    }
    if let Some(password) = password {
        client = client.with_password(password);
    }

    let handle = client
        .connect_with_subdomain(port, cli.subdomain)
        .await
        .map_err(|error| error.to_string())?;

    print_tunnel_summary(&local_url, handle.url(), password_enabled);
    tokio::signal::ctrl_c()
        .await
        .map_err(|error| error.to_string())?;
    handle.close().await;

    Ok(())
}

fn parse_local_port(local: &str) -> Result<u16, String> {
    if let Ok(port) = local.parse::<u16>() {
        return Ok(port);
    }

    if !local.contains("://") {
        let (_, port) = local
            .rsplit_once(':')
            .ok_or("local address must look like localhost:3000")?;
        return port
            .parse::<u16>()
            .map_err(|_| "local port must be a number".to_string());
    }

    if let Ok(url) = url::Url::parse(local) {
        return url
            .port_or_known_default()
            .ok_or_else(|| "local address needs a port".to_string());
    }

    Err("local address must look like localhost:3000".into())
}

fn local_url(local: &str, port: u16) -> String {
    if local.contains("://") {
        return local.to_string();
    }

    if local.parse::<u16>().is_ok() {
        return format!("http://localhost:{port}");
    }

    format!("http://{local}")
}

fn print_tunnel_summary(local_url: &str, public_url: &str, password_enabled: bool) {
    let label_width = "Public URL".len();
    let password = if password_enabled {
        "enabled"
    } else {
        "disabled"
    };
    let value_width = local_url.len().max(public_url.len()).max(password.len());
    let border = format!(
        "+{}+{}+",
        "-".repeat(label_width + 2),
        "-".repeat(value_width + 2)
    );

    println!();
    println!("Tunnel ready");
    println!();
    println!("{border}");
    println!(
        "| {:label_width$} | {:value_width$} |",
        "Local URL", local_url
    );
    println!(
        "| {:label_width$} | {:value_width$} |",
        "Public URL", public_url
    );
    println!(
        "| {:label_width$} | {:value_width$} |",
        "Password", password
    );
    println!("{border}");
    println!();
    println!("Press Ctrl+C to stop.");
    println!();
}

#[cfg(test)]
mod tests {
    use super::local_url;

    #[test]
    fn local_url_adds_http_to_host_port() {
        assert_eq!(local_url("localhost:3000", 3000), "http://localhost:3000");
    }

    #[test]
    fn local_url_uses_localhost_for_port_only() {
        assert_eq!(local_url("3000", 3000), "http://localhost:3000");
    }

    #[test]
    fn local_url_keeps_existing_scheme() {
        assert_eq!(
            local_url("https://localhost:3000", 3000),
            "https://localhost:3000"
        );
    }
}
