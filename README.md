# skycoll

**skycoll** is a Bluesky/AT Protocol social-graph CLI tool — the equivalent of [twecoll](https://github.com/nickvdp/twecoll) for the ATmosphere.

It resolves identities, fetches social graphs, downloads posts and likes via CAR repo sync, reconstructs reply threads, and produces GML graph files (with optional PNG visualisations).

## Installation

### From PyPI (once published)

```bash
pip install skycoll
```

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

# Both
skycoll init j4ck.xyz --lists --labels
```

The `.dat` file now includes:
- Profile row with labels column
- `F` rows for follows
- `B` rows for followers
- `L` rows for lists (with `--lists`)
- `S` rows for starter packs

### `fetch`

Fetch the follows of every person listed in `<handle>.dat`. Writes one `fdat/<friend>.dat` per followed user.

```bash
skycoll fetch j4ck.xyz
```

### `posts`

Download all posts from the user's AT Protocol repository via **CAR sync** (no artificial cap). Writes `<handle>.twt` with columns:

```
type    uri    timestamp    reply_to_uri    root_uri    text
```

Where `type` is `post`, `repost`, or `quote`.

```bash
# CAR sync (default, unlimited posts)
skycoll posts j4ck.xyz

# Legacy feed-based approach (limited to ~3000 items)
skycoll posts j4ck.xyz --feed
```

**CAR sync approach:** Uses `com.atproto.sync.getRepo` to download the entire repository as a CAR (Content Addressable aRchive) file. This contains every record the user has ever created — posts, reposts, likes, follows, and more. The tool parses the CAR, extracts `app.bsky.feed.post` and `app.bsky.feed.repost` records, and writes them to the `.twt` file. Because this is a one-shot download rather than paginated API calls, there's no limit on the number of posts.

**Feed approach (`--feed`):** Uses `app.bsky.feed.getAuthorFeed` with cursor-based pagination. This includes reposts and quote posts in a single feed but is capped at approximately 3000 items by the API. Use this only if CAR sync doesn't work for your PDS.

### `likes`

Download all likes. Writes `<handle>.fav` (tab-separated: `uri timestamp author_did author_handle text`).

```bash
skycoll likes j4ck.xyz
```

**Purge** (deletes all likes — the only write operation):

```bash
skycoll likes j4ck.xyz --purge
```

### `threads`

Reconstruct reply threads from an existing `<handle>.twt` file. Uses the `reply_to_uri` and `root_uri` fields to build thread trees. Outputs `<handle>.threads` as a JSON array.

```bash
skycoll threads j4ck.xyz
```

This is possible reliably in AT Protocol because every reply contains both the parent URI and the thread root URI.

### `edgelist`

Generate `<handle>.gml` from `.dat` and `fdat/` data. If `python-igraph` is installed, also renders a `<handle>.png` visualisation.

```bash
skycoll edgelist j4ck.xyz
```

The GML now includes:
- **Bidirectional edges** from both `getFollows` and `getFollowers`
- **`mutual_only` edge attribute**: `0` if the follow is mutual (both directions), `1` if one-directional
- **Starter packs as nodes** with `node_type "starter_pack"`
- **`node_type` node attribute**: `"person"` or `"starter_pack"`

### `sync`

Download the full repo CAR and write it to `<handle>.car` for archival. No parsing.

```bash
skycoll sync j4ck.xyz
```

## File formats

| File | Format |
|------|--------|
| `<handle>.dat` | Tab-separated: profile header + `F`/`B`/`L`/`S` prefixed rows |
| `fdat/<handle>.dat` | Same format as `.dat`, one file per followed user |
| `<handle>.twt` | Tab-separated: `type uri timestamp reply_to_uri root_uri text` |
| `<handle>.fav` | Tab-separated: `uri timestamp author_did author_handle text` |
| `<handle>.threads` | JSON array of thread trees (root + nested replies) |
| `<handle>.gml` | Graph Modeling Language file with `mutual_only` and `node_type` |
| `<handle>.car` | Raw CAR archive (binary) |
| `img/<handle>` | Avatar image |

## Authentication details

- **PKCE**: S256 code challenge method (mandatory)
- **DPoP**: ES256 keypair; separate nonces for auth server vs PDS
- **Scopes**: `atproto transition:generic`
- **Client type**: Public/native — loopback redirect URI on a random port
- **Client metadata**: Served from the loopback server at `/client-metadata.json`
- **Session storage**: `~/.skycoll/sessions/<did>.json` (mode `0600`)
- **`sub` verification**: Token exchange verifies the `sub` claim matches the expected DID

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