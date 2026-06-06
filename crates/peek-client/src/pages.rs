use std::fmt::Display;

const ERROR_TEMPLATE: &str = include_str!("pages/error.html");
const ERROR_CSS: &str = include_str!("pages/error.css");
const STATUS_TEMPLATE: &str = include_str!("pages/status.html");
const STATUS_CSS: &str = include_str!("pages/status.css");

pub fn status(title: &str) -> String {
    let title = escape_html(title);
    STATUS_TEMPLATE
        .replace("{{CSS}}", STATUS_CSS)
        .replace("{{TITLE}}", &title)
}

pub fn gateway_timeout(port: u16) -> String {
    error_page(
        "504 Gateway Timeout",
        &format!("localhost:{port} did not respond within 30 seconds"),
        None,
    )
}

pub fn bad_gateway(port: u16, error: &impl Display) -> String {
    error_page(
        "502 Bad Gateway",
        &format!("Could not connect to localhost:{port}"),
        Some(&error.to_string()),
    )
}

fn error_page(title: &str, message: &str, detail: Option<&str>) -> String {
    let detail_html = detail.map_or_else(String::new, |detail| {
        format!(r#"<p class="detail">{}</p>"#, escape_html(detail))
    });
    ERROR_TEMPLATE
        .replace("{{CSS}}", ERROR_CSS)
        .replace("{{TITLE}}", &escape_html(title))
        .replace("{{MESSAGE}}", &escape_html(message))
        .replace("{{DETAIL}}", &detail_html)
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::{bad_gateway, escape_html};

    #[test]
    fn escapes_html_control_characters() {
        assert_eq!(
            escape_html(r#"<script x="1">'&</script>"#),
            "&lt;script x=&quot;1&quot;&gt;&#39;&amp;&lt;/script&gt;"
        );
    }

    #[test]
    fn bad_gateway_escapes_error_detail() {
        let html = bad_gateway(3000, &"<network>");
        assert!(html.contains("&lt;network&gt;"));
        assert!(!html.contains("<network>"));
    }
}
