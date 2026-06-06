use axum::response::{Html, IntoResponse, Response};

const PASSWORD_TEMPLATE: &str = include_str!("pages/password.html");
const PASSWORD_CSS: &str = include_str!("pages/password.css");
const STATUS_TEMPLATE: &str = include_str!("pages/status.html");
const STATUS_CSS: &str = include_str!("pages/status.css");

pub fn password(error: Option<&str>) -> Response {
    let error_html = error.map_or_else(String::new, |message| {
        format!(r#"<p class="error">{}</p>"#, escape_html(message))
    });
    let html = PASSWORD_TEMPLATE
        .replace("{{CSS}}", PASSWORD_CSS)
        .replace("{{ERROR}}", &error_html);
    Html(html).into_response()
}

pub fn status(message: &str) -> Response {
    let message = escape_html(message);
    let html = STATUS_TEMPLATE
        .replace("{{CSS}}", STATUS_CSS)
        .replace("{{MESSAGE}}", &message);
    Html(html).into_response()
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
    use super::escape_html;

    #[test]
    fn escapes_html_control_characters() {
        assert_eq!(
            escape_html(r#"<script x="1">'&</script>"#),
            "&lt;script x=&quot;1&quot;&gt;&#39;&amp;&lt;/script&gt;"
        );
    }
}
