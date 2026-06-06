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

    let mut server_url = env::var("PEEK_SERVER").ok();
    let mut domain = env::var("PEEK_DOMAIN").ok();
    let mut token = env::var("PEEK_TOKEN").ok();
    let mut password = env::var("PEEK_PASSWORD").ok();
    let mut subdomain = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => server_url = Some(args.next().ok_or("--server needs a value")?),
            "--domain" => domain = Some(args.next().ok_or("--domain needs a value")?),
            "--token" => token = Some(args.next().ok_or("--token needs a value")?),
            "--password" => password = Some(args.next().ok_or("--password needs a value")?),
            "--subdomain" => subdomain = Some(args.next().ok_or("--subdomain needs a value")?),
            _ => return Err(format!("unknown option: {arg}")),
        }
    }

    let server_url = server_url
        .or_else(|| domain.map(|domain| format!("wss://{domain}/tunnel")))
        .ok_or("missing --server, --domain, PEEK_SERVER, or PEEK_DOMAIN")?;
    let password = password.unwrap_or_else(generate_password);
    let visitor_password = password.clone();

    let mut client = TunnelClient::new(&server_url).with_password(password);
    if let Some(token) = token {
        client = client.with_token(token);
    }

    let handle = client
        .connect_with_subdomain(port, subdomain)
        .await
        .map_err(|error| error.to_string())?;

    println!("{}", handle.url());
    println!("password: {visitor_password}");
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
    eprintln!("  --server <url>");
    eprintln!("  --domain <domain>");
    eprintln!("  --token <token>");
    eprintln!("  --password <password>");
    eprintln!("  --subdomain <name>");
    eprintln!();
    eprintln!("environment:");
    eprintln!("  PEEK_SERVER");
    eprintln!("  PEEK_DOMAIN");
    eprintln!("  PEEK_TOKEN");
    eprintln!("  PEEK_PASSWORD");
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

fn generate_password() -> String {
    format!("peek-{:016x}", rand::random::<u64>())
}
