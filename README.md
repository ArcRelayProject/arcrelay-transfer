# arcrelay-transfer

`arcrelay-transfer` is ArcRelay's LAN file-transfer bounded context. It uses the
process-wide network runtime and root device trust while keeping transfer
workflow, receive policy, and file validation inside this crate.

## DDD boundaries

- `domain`: peer, receive-policy, file/transfer entities, progress invariants,
  and the transfer state machine.
- `application`: send/approve/reject/pause/resume/cancel use cases plus
  repository and preview ports.
- `infrastructure`: transfer history and receive-policy persistence, bounded
  image thumbnails, hashing, path validation, staging, and atomic finalization.

## Security model

- QUIC/TLS, `_arcrelay._udp` discovery, Ed25519 identity, pairing, and grant
  enforcement are owned by `arcrelay-network`.
- File transfer opens a `FileTransfer` session on the shared endpoint and
  requires the corresponding central capability grant.
- Unpaired senders cannot open a transfer session. Automatic receive is a
  separate per-peer convenience policy and never establishes cryptographic
  trust.
- Preview bytes are bounded JPEG thumbnails carried in the offer and are never
  written as original files.
- Destination paths are relative, sanitized, checked for symlink traversal,
  written to a private staging file, size/SHA-256 verified, then atomically
  renamed into the user-selected receive directory.

The crate persists transfer history and paired-peer receive policy. Live
pause/resume is supported; interrupted application sessions are recorded as
failed and can be retried as a new transfer.
