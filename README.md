![peek banner](assets/banner.png)

**Self-hosted public URLs for localhost.**

peek gives your local server a public URL through your own hosted proxy.

```text
localhost:3000 -> your peek server -> public URL
```

## Host

[ghcr.io/sasicodes/peek](https://github.com/sasicodes/peek/pkgs/container/peek)

```bash
docker run -p 8080:8080 \
  -e PEEK_DOMAIN=example.com \
  -e PEEK_AUTH_TOKEN=change-me \
  ghcr.io/sasicodes/peek:latest
```

Point these DNS records to the server:

```text
example.com
*.example.com
```

## Run

```bash
cargo install --git https://github.com/sasicodes/peek peek-client
```

```bash
export PEEK_DOMAIN=example.com
export PEEK_AUTH_TOKEN=change-me

peek localhost:3000
```

peek prints the public URL and visitor password.
