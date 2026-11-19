# Postmortems

One file per incident that delayed or corrected a published business date, or
that would have if a control had not held: `<short-name>.md`. Blameless:
describe what the system and the procedure allowed, not who did what wrong.

| Incident | Published data affected |
|---|---|
| [Card processor release changes the transaction time format](card-date-format.md) | None; business date 2022-09-08 published late |

## Template

```markdown
# <Title: what broke, in the words of the people affected>

| | |
|---|---|
| Business dates affected | |
| Severity | SEV1 wrong data published / SEV2 date published late / SEV3 no impact on publication |
| Status | Resolved, actions open / closed |

## Summary
Three sentences: what happened, impact, how it was resolved.

## Impact
On published data, on compliance (alerts late, report stale), on other teams.

## Timeline (UTC)
| Time | Event |
|---|---|

## Detection
How it was noticed, how long after it started, and how it should have been.

## Root cause
The chain of causes, down to the one that, fixed, prevents the class of incident.

## What went well / what did not

## Actions
| Action | Type (prevent, detect, mitigate) | Owner | Status |
|---|---|---|---|
```
