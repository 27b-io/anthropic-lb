# Client store: reader protocol and formats

Status: specification. The proxy does not read a client store yet.

A client store keeps a fleet's ordinary clients in one place, so a key, a budget or a model list changes without a redeploy. Each proxy instance is a *reader*: it polls the store and enforces what it receives, and it cannot write. This page fixes what a store and its readers exchange, so that the two sides can be built and tested apart. [`tests/client-store/`](../tests/client-store/) holds the corpus both sides test against.

A store keeps two things, and every poll answer carries both:

- the **document**, which grants: the clients, their keys and their policies;
- the **restrictions**, which deny: disabled names, and the digests of revoked keys.

A reader replaces its document whole, after checking it. It never replaces its restrictions: it merges each restrictions part into the set it holds, name by name. A store that loses or rewinds a restriction therefore cannot lift it on a running reader.

## Common values

| Value | Form |
|---|---|
| Name | 1 to 63 characters from `a`-`z`, `0`-`9`, `_` and `-`, starting with a letter or a digit: `^[a-z0-9][a-z0-9_-]{0,62}$`. Names starting with `_` are reserved. Client names, the endpoint names a client is pinned to, and record names all follow this rule, the one `[[clients]]` names follow. A config file does not check endpoint names, so an endpoint whose name breaks the rule cannot be pinned from a store. |
| Digest | A SHA-256 value as 64 lowercase hex characters: `^[0-9a-f]{64}$`. |
| Timestamp | UTC with milliseconds, exactly as JavaScript's `Date.prototype.toISOString` writes it, for example `2026-10-11T06:26:40.123Z`: `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$`. Only the pattern is checked. |
| Integer | A JSON number written as decimal digits only, with no sign, fraction or exponent, from 0 to 2^53−1 (9007199254740991) unless a rule narrows the range. 2^53−1 is JavaScript's `Number.MAX_SAFE_INTEGER`, the largest integer it never confuses with another. |
| Model pattern | 1 to 128 printable ASCII characters (`!` to `~`) other than `"`, `,` and `\`, in which `*` appears at most once and only as the last character. `claude-haiku-*` matches every model that starts with `claude-haiku-`, and `*` alone matches every model. |

Every pattern above matches the whole value: a trailing line feed breaks it. Sizes count UTF-8 bytes. Every bounded value is ASCII, so its length in characters is also its length in bytes.

The empty pattern, which a `[[clients]]` entry can use to deny every model, is not allowed: a store client is denied by disabling its name.

A store checks parsed values, and it writes every number as JavaScript's `JSON.stringify` does. Input that only a hand edit could produce therefore never reaches a reader from a store: a number spelled `1.0`, `6e8`, `-0` or `1E400`, a fraction that rounds to an integer, a decimal finer than a double holds, a lone surrogate, nesting deeper than the format uses, or a member name repeated within one object. A reader may refuse such a document whole where a JavaScript check would quarantine one client, and no corpus case depends on any of them.

## The document

The store keeps its document as the exact bytes it wrote. The document's digest is the SHA-256 of those bytes. Schema 1, with whitespace added here for reading:

```json
{
  "schema": 1,
  "version": 1791700000123,
  "updated_at": "2026-10-11T06:26:40.123Z",
  "clients": [
    {
      "name": "acme",
      "daily_budget": 600000000,
      "utilization_limit": null,
      "models": [],
      "preferred_endpoints": [],
      "keys": [
        {
          "id": "k7f3a2c1",
          "label": "2026-10 rotation",
          "digest": "<digest>",
          "created_at": "2026-10-01T09:30:00.000Z"
        }
      ]
    }
  ],
  "revoked_digests": ["<digest>"]
}
```

| Member | Meaning |
|---|---|
| `schema` | The document's format. 1 for this one. |
| `version` | When the store wrote the document, in Unix milliseconds of its clock. Versions only rise. A store at version 0 holds no document. |
| `updated_at` | When the store wrote the document. Informational. |
| `clients` | The clients. |
| `clients[].name` | The client's name. |
| `clients[].daily_budget` | The client's daily token budget, with the meaning of a config file's `client_budgets`, or `null` for none. |
| `clients[].utilization_limit` | The client's utilization ceiling, with the meaning of `client_utilization_limits`, or `null` for none. |
| `clients[].models` | The model patterns the client may request, with the meaning `models` has in a config file's `[[clients]]` entry. An empty list allows every model. |
| `clients[].preferred_endpoints` | The endpoints the client is pinned to, with the meaning `preferred_endpoints` has in a `[[clients]]` entry. A reader drops a name its own configuration lacks, with a warning. |
| `clients[].keys` | The client's keys. Two at most, so a key can rotate with overlap. |
| `clients[].keys[].id` | A short handle for the key, for logs. |
| `clients[].keys[].label` | Free text chosen by the operator. |
| `clients[].keys[].digest` | The key's digest (see [Key digests](#key-digests)). |
| `clients[].keys[].created_at` | When the store minted the key. |
| `revoked_digests` | Digests of revoked keys. A reader that installs the document adds them to the digests it holds, which never shrink (see [Merging a part](#merging-a-part)). |

The store writes the document compactly, with the members in the order shown and the clients in ascending byte order of name. Readers do not depend on either order.

### Checks

A reader checks a document before it installs it, and a store applies the same checks before it writes one.

A reader sorts each fault by its reach. An **envelope** fault, a schema the reader does not read among them, refuses the whole document: the reader keeps its last good document, or stays without one, and alerts. A **client** fault quarantines the client that holds it: the reader refuses every key of that name, its config-file keys included, installs the rest of the document, and alerts. Everything a reader needs to map each key digest to exactly one name is envelope, so a quarantine can never leave a key pointing at the wrong client.

| Envelope rule | The document is refused unless |
|---|---|
| `document.size` | its text is at most 2 MiB (2,097,152 bytes). |
| `document.object` | its text parses as JSON, and is an object. |
| `document.schema` | its `schema` is the integer 1. Checked before any other member, so a reader that meets a newer schema reports the schema, not the members it does not know. |
| `document.members` | its members are exactly `schema`, `version`, `updated_at`, `clients` and `revoked_digests`. |
| `document.version` | `version` is an integer from 1 to 2^53−1. |
| `document.updated_at` | `updated_at` is a timestamp. |
| `document.clients` | `clients` is an array of at most 64 objects. |
| `document.name` | every client's `name` is a name, and no two clients share one. |
| `document.keys` | every client's `keys` is an array of objects, and each of them has a `digest` member. |
| `document.revoked_digests` | `revoked_digests` is an array. |
| `document.digest` | every key's `digest`, and every entry of `revoked_digests`, is a digest. |
| `document.digest_once` | no digest appears twice across all keys and `revoked_digests`. |

| Client rule | The client is quarantined unless |
|---|---|
| `client.members` | its members are exactly `name`, `daily_budget`, `utilization_limit`, `models`, `preferred_endpoints` and `keys`. |
| `client.daily_budget` | `daily_budget` is `null` or an integer from 1 to 2^53−1. |
| `client.utilization_limit` | `utilization_limit` is `null` or a number above 0 and at most 1. |
| `client.models` | `models` is an array of at most 64 model patterns. |
| `client.preferred_endpoints` | `preferred_endpoints` is an array of at most 16 names. |
| `client.keys` | `keys` holds at most 2 keys. |
| `key.members` | each key's members are exactly `id`, `label`, `digest` and `created_at`. |
| `key.id` | each key's `id` is `k` followed by 7 lowercase hex digits, and no two keys of the client share one. |
| `key.label` | each key's `label` is 0 to 64 printable ASCII characters, from the space to `~`. |
| `key.created_at` | each key's `created_at` is a timestamp. |

An envelope fault refuses the document whatever else it holds. When a document breaks more than one rule, the rule reported is an envelope rule if any is broken, and otherwise the first in its table. Faults in two different clients, or two different keys, are not ordered against each other.

One rule sits outside the format, because its input differs per deployment. A store refuses a client that takes the name of one of the deployment's operators or admin readers. A reader that finds such a client in a document anyway applies the envelope rules to the whole document, that client included, then drops the client before any client rule, and alerts: none of its keys enter the reader's lookup, it is never quarantined, and the reader keeps its own principal for the name.

A name in both the document and a reader's config file is one client on that reader: its keys from both authenticate, and the document's policy governs them. `null` and `[]` are values the document carries. Only a setting that schema 1 does not have, such as a guard policy, keeps its config-file value.

A reader installs a document only when the document passes these checks, its version is above the one the reader holds, and the version is at most 15 minutes ahead of the reader's clock. A version further ahead raises a clock-skew alert.

### Key digests

A key's digest is the SHA-256 of the key's text, its UTF-8 bytes, as a digest. A store mints a key as 32 random bytes and hands it out once as unpadded base64url, 43 characters. It keeps the digest of those 43 characters, not of the raw bytes. A reader therefore digests every presented key the same way, whether the key came from the store or from its own config file, and one lookup serves both. So a proxy that reads a store holds its config-file keys as SHA-256 digests too.

When a store key's digest equals that of any key in the reader's config file, the reader quarantines the store client and alerts. A store never holds a config-file key's digest, so only an edit or a hand-written import can cause this. A config-file key is never handed to another principal: under another name it keeps working for its own, and under the same name it falls with the quarantine.

## The restrictions part

A restrictions part carries the store's restrictions as a *body*, a JSON text, and a *tag*, the SHA-256 of the body's UTF-8 bytes as a digest. A body, with whitespace added here for reading:

```json
{
  "full": true,
  "enabled_ahead": 0,
  "records": [
    {"name": "acme", "state": "disabled", "version": 1791700000123000},
    {"name": "batch-7", "state": "enabled", "version": 1791700000456000}
  ],
  "revoked_digests": ["<digest>"]
}
```

| Member | Meaning |
|---|---|
| `full` | `true` only when the store's integrity record, which its own writes keep beside its restrictions, matches every row it read, and the body carries every one of those rows except the enabled records that `enabled_ahead` counts. A write updates the integrity record only when the record matched before the write, and only a deliberate reseed of the table creates or repairs one. An empty table with a matching record is full; a wiped one is partial. |
| `enabled_ahead` | How many enabled records the store left out because they are more than 60 s ahead of its clock. A reader alerts while it is above 0. |
| `records` | One record per name: the name, its `state` (`disabled` or `enabled`), and the `version` at which the state was set, in Unix microseconds of the store's clock. |
| `revoked_digests` | Digests of revoked keys. |

A store stamps every record above the previous record of its name, so a later write never ties with an earlier one or loses to it. It writes the body compactly, with the members in the order shown, the records in ascending byte order of name and the digests in ascending order, so an unchanged table keeps its tag. Readers do not depend on that order.

| Rule | The part is refused unless |
|---|---|
| `part.size` | the body is at most 2 MiB (2,097,152 bytes). |
| `part.tag` | its tag is the SHA-256 of the body's UTF-8 bytes, as a digest. |
| `part.object` | the body parses as JSON, and is an object. |
| `part.members` | the body's members are exactly `full`, `enabled_ahead`, `records` and `revoked_digests`. |
| `part.full` | `full` is `true` or `false`. |
| `part.enabled_ahead` | `enabled_ahead` is an integer. |
| `part.records` | `records` is an array of objects, and each one's members are exactly `name`, `state` and `version`. |
| `part.record_name` | every record's `name` is a name, and no two records share one. |
| `part.record_state` | every record's `state` is `disabled` or `enabled`. |
| `part.record_version` | every record's `version` is an integer from 1 to 2^53−1. |
| `part.revoked_digests` | `revoked_digests` is an array of digests, and none appears twice. |

The rules are checked in the order of the table, so the size and the tag are checked before the body is parsed, and a fault is reported as the first rule it breaks.

A store never sends a body that breaks a rule. It leaves out any row that would break one, which only an edit can leave behind, and marks the part partial, so that one bad row cannot stop every later restriction from reaching readers. A new body format, like a new document schema, reaches every reader before any store writes it.

A reader puts no limit on how many records or digests a part holds. A part that denies more than a store would allow still only denies, and refusing it would also stop every later restriction from arriving. A store caps its own table at 1,024 disabled records, 4,096 enabled records and 4,096 digests, which keeps a body under 1 MB.

## The set hash

The set hash is a function of a set of records and digests alone, so a reader and a store that hold the same set compute the same hash. It covers each record's name, state and version, and every revoked digest. A set's completeness mark travels beside its hash, never inside it.

The *set text* has one line per record, in ascending byte order of name, then one line per digest, in ascending order. Each line ends with a line feed (0x0A):

```text
r <name> <state> <version>
d <digest>
```

`<version>` is in decimal, with no sign and no leading zeros. Byte order is plain code-point order on these ASCII strings: JavaScript's default `sort()` gives it, and `localeCompare` does not. The set hash is the SHA-256 of the set text, as a digest. For example, this set text has the hash `5d3eb4669447223b4441b22f80712c129abe2ad435a6961d7f027287126c4951`:

```text
r acme disabled 1791700000123000
r batch-7 enabled 1791700000456000
d 1111111111111111111111111111111111111111111111111111111111111111
```

The empty set's text is empty, so its hash is `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

The set text is also the form in which to log a set or hand one back to a store, because its hash checks it byte for byte before anything parses it.

## Merging a part

A reader merges every part it accepts into the set it holds. Each record in the part meets the first of these rules that applies to it; the order of the records does not matter.

1. An `enabled` record from a partial part is skipped. A damaged table can add denials but never lift one.
2. An `enabled` record more than 15 minutes (900,000,000 µs) ahead of the reader's clock is skipped, and the reader raises a clock-skew alert. While it skips one, the reader polls without a tag, so that every answer carries the body and the record merges once the clocks agree. A `disabled` record is never skipped for being ahead, because skipping it could only lift a denial.
3. Otherwise the record replaces the one the reader holds for its name when the reader holds none, when the record's version is higher, or when the versions are equal and the record is `disabled` while the held one is `enabled`. A `disabled` record wins a tie.

Then:

4. When the part is full, the reader forgets every `enabled` record it holds whose name has no record in the part, skipped or not. That changes nothing it enforces. A `disabled` record is never forgotten: only a newer `enabled` record replaces it.
5. Every digest in the part joins the digests the reader holds, as do the `revoked_digests` of every document it installs. None ever leaves.
6. The set becomes complete once it has merged a full part, and stays complete. A partial part never makes a set complete. A full part does even when it skipped a record for being ahead: a skipped record is `enabled`, so missing it can only keep a denial in place.

While the last part the reader merged lacks a disable or a digest that the reader holds, the reader raises a restrictions-behind alert. Every record in that part counts, skipped or not. The part lacks a disable the reader holds when it has neither a `disabled` record for that name at the same version or a higher one, nor an `enabled` record for it at a higher version. It lacks a digest the reader holds when it does not list it. The reader also alerts while the last part it merged is partial.

Until its set is complete, a reader serves no client but its operators and admin readers, whether the client comes from the store or from its config file. Once it is complete, the reader enforces the set whether or not it holds a document:

- A `disabled` record refuses every key of its name, from the store or the reader's config file. The only exception is a name the reader keeps for an operator or an admin reader, which a record never touches; the reader alerts instead.
- A listed digest refuses the store key it matches. A store revokes only keys it minted or imported, so a listed digest that matches a key in the reader's config file is ignored there, with an alert.

## The poll

A reader polls `GET <poll URL>` every few seconds. Only `version` and `tag` choose what the answer carries:

| Parameter | Value |
|---|---|
| `version` | The version of the document the reader holds, or 0. |
| `tag` | The tag of the last part the reader merged. Left out to ask for the body: from the moment the reader starts until it has merged a part, and while it skips a record that is ahead. |

The other four are the reader's report:

| Parameter | Value |
|---|---|
| `digest` | The digest of the document the reader holds. Left out when it holds none. |
| `records_version` | The highest version among the records the reader holds, leaving out any more than 15 minutes ahead of its clock, or 0. |
| `set_hash` | The hash of the set the reader holds. |
| `set_complete` | `true` or `false`: the set's completeness mark. |

`version` and `records_version` are integers, `digest`, `tag` and `set_hash` are digests, and `set_complete` is `true` or `false`. A store ignores parameters it does not know. It treats a parameter that appears twice or breaks its form as absent, an absent `version` or `records_version` as 0, and an absent `set_complete` as `false`, so a malformed poll gets more of the answer, never less.

The poll travels over HTTPS with the store's certificate validated, or over a channel that never leaves the reader's own platform. The digest and the tag catch corruption, not tampering: anyone who can rewrite an answer can recompute both.

The reader sends a `User-Agent` naming its software and version, `anthropic-lb/<version>` for this proxy, and `Authorization: Bearer <token>` when it is configured with a token. A store that requires a token answers 401 to a missing or wrong one before it reads anything else. Otherwise it answers 200 with both parts, whatever state they are in:

```json
{
  "document": {"status": "ok", "version": 1791700000123, "digest": "<digest>", "text": "<document>"},
  "restrictions": {"status": "ok", "tag": "<digest>", "body": "<body>"}
}
```

- A part the store could not read in time is `{"status": "unavailable"}`, and the other part is still served. A store also serves its document part as unavailable when the text is larger than 2 MiB, which only an edit can produce.
- The document part carries `version`, and `digest` unless `version` is 0, when `digest` is absent. It carries the document's exact text as the string `text` when the reader's `version` differs from the store's and the store's is not 0.
- The restrictions part carries `tag`. It carries the body as the string `body` when the reader sent no `tag` or a different one.

Both texts travel as JSON strings, so each side hashes exactly the bytes the store wrote, however either side re-serialises JSON.

A store records a reader's report only from a channel that no other caller can use. A report that arrives with a bearer token is never recorded, and never changes what a store serves or what it accepts.

A reader checks each part on its own, so a fault in one part never refuses the other. Decoding each part separately, for example as a raw JSON value, keeps a bad document text from spoiling the restrictions. A reader ignores members it does not know in the answer and in each part, and refuses a part whose `status` is neither `ok` nor `unavailable`.

- **The document part.** `version` is an integer, and `digest` is a digest unless `version` is 0.
  - When `version` is above the one the reader holds, `text` must be present, the SHA-256 of its UTF-8 bytes must equal `digest`, and it must pass the [document checks](#checks). Its own `version` must equal the part's. The install rules above then decide.
  - When `version` equals the reader's, a different `digest` means the store was changed outside its write path.
  - When `version` is below the reader's, the store is behind.
  - In either of the last two cases the reader keeps its document and alerts. A store at version 0 is behind every reader that holds a document.
- **The restrictions part.** `tag` is a digest. When it differs from the tag of the last part the reader merged, or the reader sent no `tag`, `body` must be present and pass the part rules before the reader merges it.

A part that fails a check is refused like an unavailable one: the reader keeps what it holds, and alerts. An answer the reader cannot read, a status other than 200, or an answer larger than 16 MiB is a failed poll: the reader keeps everything it holds and polls again.

## The corpus

[`tests/client-store/`](../tests/client-store/) holds four files. Each is a JSON object that holds its cases in arrays: `cases`, or `set_hashes` and `key_digests` in `hashes.json`. Every case has an `id`, unique in its array, and a `why`, and states its expected outcome. A failing case states the rule it breaks; where it breaks more than one, the one reported first in check order. A test should check the rule as well as the outcome, so that no case passes for the wrong reason. The size rules and the poll exchange have no cases: each side tests them on its own.

| File | Case | Expected outcome |
|---|---|---|
| `documents.json` | `document`: a document, as a JSON value. | `"install"`, `{"refuse": "<rule>"}`, or `{"quarantine": {"<name>": "<rule>"}}`. `"install"` means the document passes every check: whether a reader installs it also depends on the version the reader holds and on its clock, which the cases leave out. A store accepts exactly the `install` cases. |
| `parts.json` | `tag`, and `body` as a string. | `"accept"` or `{"refuse": "<rule>"}`. Every accepted body but `unsorted-accepted` is written as a store writes one. |
| `merges.json` | `now_us`, the reader's clock in Unix microseconds; `held`, the set before: `complete`, `records`, `revoked_digests`; and `part`: `full`, `records`, `revoked_digests`. | `held`, the set after, with its `set_hash`; `records_version`, as the reader then reports it; `skipped_ahead`, the names of the records skipped for being ahead; and `behind`, what the store lacks: `disabled` names and `revoked_digests`. |
| `hashes.json` | `set_hashes`: `records` and `revoked_digests` in any order. `key_digests`: `bytes_hex`, a key's raw bytes in hex. | `set_hashes`: the set `text` and its `hash`. `key_digests`: the key's `base64url` text, as a store hands it out, and its `digest`. |

Every list in an expected outcome is in ascending order. Each version of the corpus is tagged `client-store-corpus-v<N>` in this repository. Vendor a copy at a tag, and record the tag and its commit.
