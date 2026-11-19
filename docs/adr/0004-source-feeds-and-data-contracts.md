# ADR 0004: Source feeds, ground truth and data contracts

- Status: accepted

## Context

The platform ingests four sources with different delivery mechanisms: a daily
payments hub file, daily card processor files, the core banking database and
two public sanctions lists. Detection rules must be measured against labelled
outcomes, and every later layer depends on knowing exactly what each source
promises.

## Decision

1. **Every file feed carries its own completeness signal.** Payments files are
   followed by a manifest (record count, SHA-256) written after the data file;
   card files end with a trailer (record count, amount total). Files are written
   under a temporary name and renamed, so a reader never sees a partial file.
2. **Labels never enter the platform.** The IBM `Is Laundering` flag and the
   laundering typologies, the duplicate customer records and the customers
   matching sanctioned persons are written to `data/ground_truth/`, which no
   pipeline reads. They are used only to compute precision and recall.
3. **Sources keep their defects.** Bank identifiers in the payments feed stay
   zero-padded with inconsistent widths, the core banking holds duplicate
   records of the same party, and card files can be resent or truncated
   (`--defect`). Correcting them is the platform's job, not the source's.
4. **Sources are reproducible.** Customer master data is derived per IBM party
   with a fixed seed, and IBM inputs are pinned by SHA-256, so every figure
   reported by the platform can be reproduced byte for byte.
5. **One contract per source, in Open Data Contract Standard v3.1** under
   `contracts/`. Tests assert that each producer emits exactly the contracted
   structure and that every contract validates against the ODCS JSON Schema.
   Ingestion jobs derive their schemas from the contracts.
6. **Sanctions downloads are validated before landing**: well-formed XML, a
   minimum entry count, the publisher's declared count (OFAC), and no drop of
   more than 10% against the previous snapshot without an explicit override.

## Alternatives considered

| Option | Why not |
|---|---|
| Keep the label in the payments feed and drop it in silver | One forgotten `SELECT *` leaks the answer into the rules and inflates every metric |
| Clean the identifiers at the source | Hides the reconciliation work the platform exists to do |
| `_SUCCESS` marker file for the payments feed | Says the file is complete but not what it contains; a manifest also detects truncation and corruption |
| Contracts as JSON Schema or Pydantic models | Not a data contract standard; ODCS also carries ownership, SLAs and quality rules, and is read by contract tooling |
| `datacontract-cli` as a dependency | Large dependency tree for what is, at this stage, schema validation |

## Consequences

- Rebuilding the sources from scratch takes about three minutes and gives the
  same bytes, which makes a full-history regeneration test possible.
- Ground truth must never be mounted into the Spark or Trino containers.
- A sanctions publisher that legitimately removes many entries requires a
  human decision (`--allow-shrink`), by design.
