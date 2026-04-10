# skycoll

**skycoll** is a Bluesky/AT Protocol social-graph CLI tool — the equivalent of [twecoll](https://github.com/nickvdp/twecoll) for the ATmosphere.

It resolves identities, fetches social graphs, downloads posts and likes via CAR repo sync, reconstructs reply threads, and produces GML graph files (with optional PNG visualisations).

## Installation

### From source

```bash
git clone https://github.com/j4ckxyz/skycoll.git
cd skycoll
pip install -e .

# Optional: graph visualisation support
pip install -e ".[graph]"
```

### Dependencies

Core: `httpx`, `atproto`, `cryptography`, `cbor2`

Optional: `python-igraph` (for PNG graph rendering)

Dev: `pytest`, `pytest-httpx`

## First-run OAuth flow

The first time you run a command that requires authentication (`init`, `fetch`, `posts`, `likes`), skycoll will:

1. Resolve your handle to a DID and PDS endpoint.
2. Discover the OAuth 2.0 authorisation server from your PDS.
3. Start a temporary HTTP server on `127.0.0.1:<random-port>` to serve the client metadata document and receive the callback.
4. Open your browser for you to authorise the request (scopes: `atproto transition:generic`).
5. Exchange the authorisation code using PKCE (S256) and bind it with DPoP (ES256).
6. Save the session to `~/.skycoll/sessions/<did>.json` (mode `0600`).

On subsequent runs, the saved session is reused and refreshed automatically when within 60 seconds of token expiry.

> **NOTE:** The `transition:generic` scope provides read access and like-deletion. When AT Protocol proposal 0011 (granular scopes) stabilises, this should be narrowed to only `app.bsky.feed.*` reads and `app.bsky.feed.like` delete.

## Commands

### `resolve`

Resolve a handle to a DID (or a DID to a handle + PDS endpoint).

```bash
skycoll resolve j4ck.xyz
skycoll resolve did:plc:z72i7hdynmk6r22z27h6tvae
```

### `init`

Fetch your profile, follows, and followers. Writes `<handle>.dat` and downloads avatars to `img/`.

```bash
skycoll init j4ck.xyz

# Also fetch lists you've created
skycoll init j4ck.xyz --lists

# Include self-labels and server-assigned labels
skycoll init j4ck.xyz --labels

# Route through the Blacksky AppView
skycoll init j4ck.xyz --appview blacksky

# Query a Constellation backlinks index
skycoll init j4ck.xyz --constellation https://constellation.example.com

# All flags combined
skycoll init j4ck.xyz --lists --labels --appview blacksky --constellation https://constellation.example.com
```

The `.dat` file includes:
- Profile header row with labels column
- `F` rows for follows
- `B` rows for followers
- `L` rows for lists (with `--lists`)
- `S` rows for starter packs
- `K` rows for Constellation backlink counts (with `--constellation`)

By default, labels are omitted from the profile row. Use `--labels` to include
server/self labels in the `.dat` profile header.

### `appview` flag

Several commands accept `--appview` to route API requests through a specific Bluesky-compatible AppView. This sets the `atproto-proxy` HTTP header to a service DID, rather than hardcoding a base URL.

Built-in names:
| Name | Service DID | Description |
|---|---|---|
| `bluesky` | `did:web:api.bsky.app#bsky_appview` | Bluesky official AppView (default) |
| `blacksky` | `did:web:api.blacksky.community#bsky_appview` | Blacksky community AppView |

You can also pass a raw DID+fragment string for custom AppViews:
```bash
skycoll init j4ck.xyz --appview did:web:custom.example#bsky_appview
```

### `appviews`

List the built-in AppView names and their service DIDs:

```bash
skycoll appviews
```

### `fetch`

Fetch the follows of every person listed in `<handle>.dat`. Writes one `fdat/<friend>.dat` per followed user.

```bash
skycoll fetch j4ck.xyz
```

### `posts`

Download posts using paginated `getAuthorFeed` (default, no cap — pages until cursor is exhausted):

```bash
skycoll posts j4ck.xyz
```

Use `--car` for full CAR repo sync (slower but gives a complete archive including all record types):

```bash
skycoll posts j4ck.xyz --car
```

Rich `.twt` format columns: `type uri timestamp reply_to_uri root_uri text`

Where `type` is `post`, `repost`, or `quote`.

Route through an alternative AppView:
```bash
skycoll posts j4ck.xyz --appview blacksky
```

### `likes`

Download all likes. Writes `<handle>.fav` (tab-separated: `uri timestamp author_did author_handle text`).

```bash
skycoll likes j4ck.xyz
```

Purge (delete all likes — the only write operation):
```bash
skycoll likes j4ck.xyz --purge
```

Route likes reads through an alternative AppView:
```bash
skycoll likes j4ck.xyz --appview blacksky
```

## Verbose logging

Use global verbose mode to print low-level network/auth debug logs:

```bash
skycoll --verbose init j4ck.xyz
skycoll -v posts j4ck.xyz --car
```

You can also enable it via environment variable:

```bash
SKYCOLL_VERBOSE=1 skycoll init j4ck.xyz
```

### `threads`

Reconstruct reply threads from an existing `<handle>.twt` file. Uses the `reply_to_uri` and `root_uri` fields to build thread trees. Outputs `<handle>.threads` as JSON.

```bash
skycoll threads j4ck.xyz
```

### `edgelist`

Generate `<handle>.gml` from `.dat` and `fdat/` data. If `python-igraph` is installed, also renders a `<handle>.png` visualisation.

The GML includes bidirectional edges, `mutual_only` attributes, and `node_type` attributes.

```bash
skycoll edgelist j4ck.xyz

# Enrich edges with likes counts from Constellation
skycoll edgelist j4ck.xyz --constellation https://constellation.example.com
```

### `sync`

Download the full repo CAR and write it to `<handle>.car` for archival. No parsing.

```bash
skycoll sync j4ck.xyz
```

### `backlinks`

Query a [Constellation](https://github.com/at-microcosm/microcosm-rs/tree/main/constellation) backlinks index and pretty-print the full backlink breakdown for a handle.

Constellation is a self-hostable AT Protocol backlinks index. A public instance may be available; this feature is opt-in and the host must be provided explicitly.

```bash
skycoll backlinks j4ck.xyz --constellation https://constellation.example.com
```

### `plc`

Fetch the full PLC directory operation log for a DID and write it to `<did>.plc` as JSON. This gives the complete identity history — handle changes, PDS migrations, key rotations.

```bash
skycoll plc did:plc:z72i7hdynmk6r22z27h6tvae

# Also print a human-readable summary
skycoll plc did:plc:z72i7hdynmk6r22z27h6tvae --audit
```

### `firehose`

Connect to an AT Protocol relay WebSocket and stream repo events in real time. Filter by handle or DID, and optionally stop after N events.

```bash
# Stream all events from the default relay (wss://bsky.network)
skycoll firehose

# Filter by DID
skycoll firehose --did did:plc:abc123

# Filter by handle (resolved to DID automatically)
skycoll firehose --handle j4ck.xyz

# Use the Blacksky/atproto.africa relay
skycoll firehose --relay wss://atproto.africa

# Stop after 100 matching events
skycoll firehose --handle j4ck.xyz --limit 100
```

## File formats

| File | Format |
|------|--------|
| `<handle>.dat` | Tab-separated: profile header + `F`/`B`/`L`/`S`/`K` prefixed rows |
| `fdat/<handle>.dat` | Same format as `.dat`, one file per followed user |
| `<handle>.twt` | Tab-separated: `type uri timestamp reply_to_uri root_uri text` |
| `<handle>.fav` | Tab-separated: `uri timestamp author_did author_handle text` |
| `<handle>.threads` | JSON array of thread trees (root + nested replies) |
| `<handle>.gml` | Graph Modeling Language file with `mutual_only` and `node_type` |
| `<handle>.car` | Raw CAR archive (binary) |
| `<did>.plc` | PLC directory operation log (JSON) |
| `img/<handle>` | Avatar image |

## Authentication details

- **PKCE**: S256 code challenge method (mandatory)
- **DPoP**: ES256 keypair; separate nonces for auth server vs PDS
- **Scopes**: `atproto transition:generic`
- **Client type**: Public/native — loopback redirect URI on a random port
- **Client metadata**: Served from the loopback server at `/client-metadata.json`
- **Session storage**: `~/.skycoll/sessions/<did>.json` (mode `0600`)
- **`sub` verification**: Token exchange verifies the `sub` claim matches the expected DID
- **`atproto-proxy` header**: Routes requests through a specified AppView service DID
- **PAR + nonce handling**: Uses pushed authorization requests and retries with server-provided `DPoP-Nonce`

## PDS resolution

skycoll **never hardcodes bsky.social**. For every handle:

1. Resolve handle → DID via DNS `_atproto` TXT or `https://bsky.social/xrpc/com.atproto.identity.resolveHandle`
2. Fetch the DID document (`plc.directory` for `did:plc`, HTTPS well-known for `did:web`)
3. Extract the `#atproto_pds` service endpoint
4. Make all authenticated API calls against that PDS

## Pagination & rate limits

All AT Protocol list endpoints are cursor-based. skycoll loops until no cursor is returned. On HTTP 429, it backs off with exponential retry (max 3 attempts).

## Running tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## License

MIT
