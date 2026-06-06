use std::env;

use peek_client::TunnelClient;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        eprintln!();
        print_usage();
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let raw_args: Vec<String> = env::args().skip(1).collect();
    if raw_args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        std::process::exit(0);
    }

    let mut args = raw_args.into_iter();

    let local = args.next().ok_or("missing local address")?;
    let port = parse_local_port(&local)?;
    let local_url = local_url(&local, port);

    let mut server_url = env::var("PEEK_SERVER").ok();
    let mut domain = env::var("PEEK_DOMAIN").ok();
    let mut token = env::var("PEEK_TOKEN")
        .ok()
        .or_else(|| env::var("PEEK_AUTH_TOKEN").ok());
    let mut password = env::var("PEEK_PASSWORD").ok();
    let mut subdomain = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => server_url = Some(next_option_value(&mut args, "--server")?),
            "--domain" => domain = Some(next_option_value(&mut args, "--domain")?),
            "--token" => token = Some(next_option_value(&mut args, "--token")?),
            "--password" => password = Some(next_option_value(&mut args, "--password")?),
            "--subdomain" => subdomain = Some(next_option_value(&mut args, "--subdomain")?),
            _ => return Err(format!("unknown option: {arg}")),
        }
    }

    let server_url = server_url
        .or_else(|| domain.map(|domain| format!("wss://{domain}/tunnel")))
        .ok_or("missing --server, --domain, PEEK_SERVER, or PEEK_DOMAIN")?;
    let mut client = TunnelClient::new(&server_url).map_err(|error| error.to_string())?;
    if let Some(token) = token {
        client = client.with_token(token);
    }
    if let Some(password) = password {
        client = client.with_password(password);
    }

    let handle = client
        .connect_with_subdomain(port, subdomain)
        .await
        .map_err(|error| error.to_string())?;

    print_tunnel_summary(&local_url, handle.url());
    tokio::signal::ctrl_c()
        .await
        .map_err(|error| error.to_string())?;
    handle.close().await;

    Ok(())
}

fn print_usage() {
    eprintln!("usage: peek <local-address> [options]");
    eprintln!();
    eprintln!("example:");
    eprintln!("  peek localhost:3000 --domain example.com --token change-me");
    eprintln!();
    eprintln!("options:");
    eprintln!("  --server <url>        full WebSocket URL, like wss://example.com/tunnel");
    eprintln!("  --domain <domain>     hosted peek domain, like example.com");
    eprintln!("  --token <token>       server token used to create a tunnel");
    eprintln!("  --password <password> require this password for visitors");
    eprintln!("  --subdomain <name>    public URL name, like https://name.example.com");
    eprintln!();
    eprintln!("environment:");
    eprintln!("  PEEK_SERVER     full WebSocket URL");
    eprintln!("  PEEK_DOMAIN     hosted peek domain");
    eprintln!("  PEEK_TOKEN      server token used to create a tunnel");
    eprintln!("  PEEK_AUTH_TOKEN same as PEEK_TOKEN");
    eprintln!("  PEEK_PASSWORD   require this password for visitors");
}

fn next_option_value(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, String> {
    let value = args
        .next()
        .ok_or_else(|| format!("{option} needs a value"))?;
    if value.starts_with("--") {
        return Err(format!("{option} needs a value"));
    }
    Ok(value)
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

fn print_tunnel_summary(local_url: &str, public_url: &str) {
    let label_width = "Public URL".len();
    let value_width = local_url.len().max(public_url.len());
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
    println!("{border}");
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
