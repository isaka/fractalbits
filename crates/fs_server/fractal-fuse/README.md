# fractal-fuse

An async FUSE (Filesystem in Userspace) library for Linux, built on
**io_uring** and the **compio** async runtime. It uses the
`FUSE_OVER_IO_URING` kernel interface (Linux 6.14+) for high-performance
userspace filesystem I/O with zero-copy buffer registration.

## Features

- **io_uring-native FUSE transport** -- Uses `FUSE_IO_URING_CMD_REGISTER` and
  `FUSE_IO_URING_CMD_COMMIT_AND_FETCH` for zero-copy request/response cycles
- **Per-CPU queue architecture** -- Spawns one io_uring ring per CPU core with
  thread affinity for cache-friendly processing
- **Async filesystem trait** -- Implement the `Filesystem` trait with async
  methods; unimplemented operations default to `ENOSYS`
- **Unprivileged mounting** -- Uses `fusermount3` for non-root mounts
- **Configurable mount options** -- Builder-pattern `MountOptions` for
  `allow_other`, `default_permissions`, `writeback_cache`, and more
- **FUSE protocol v7.45** -- Full ABI definitions with support for
  `readdirplus`, `fallocate`, `lseek`, `copy_file_range`, and other modern
  operations

## Requirements

- **Linux 6.14+** with `FUSE_OVER_IO_URING` support enabled
- **fusermount3** installed and accessible in `$PATH`
- **Rust edition 2024** (nightly or Rust 1.85+)

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
fractal-fuse = "0.1"
```

Implement the `Filesystem` trait and run a session:

```rust
use std::path::Path;
use fractal_fuse::{Filesystem, MountOptions, Session};

struct MyFs;

impl Filesystem for MyFs {
    // Implement the operations your filesystem supports.
    // All methods default to returning ENOSYS.
}

fn main() -> std::io::Result<()> {
    let opts = MountOptions::new()
        .fs_name("myfs")
        .allow_other(true)
        .default_permissions(true);

    Session::new(opts)
        .queue_depth(128)
        .run(MyFs, Path::new("/mnt/myfs"))
}
```

## Architecture

```
Session::run()
  |
  +-- fusermount3 (mount, receive /dev/fuse fd)
  +-- FUSE_INIT handshake (blocking read/write on /dev/fuse)
  +-- Per-CPU ring threads (each with compio Runtime + thread affinity)
        |
        +-- RingEntry buffers (page-aligned, mmap'd)
        +-- FuseRegister (register buffers with kernel)
        +-- Loop: dispatch request -> FuseCommitAndFetch (respond + fetch next)
```

Each ring thread runs a compio single-threaded runtime pinned to a CPU core.
Ring entries use page-aligned `mmap` buffers for the header (288 bytes) and
payload (up to `max_write` bytes, default 1MB). The kernel fills request data
directly into these buffers, and responses are written back in-place.

## Supported FUSE Operations

| Operation | Trait Method |
|-----------|-------------|
| LOOKUP | `lookup` |
| FORGET / BATCH_FORGET | `forget` / `batch_forget` |
| GETATTR / SETATTR | `getattr` / `setattr` |
| READLINK / SYMLINK | `readlink` / `symlink` |
| MKNOD / MKDIR | `mknod` / `mkdir` |
| UNLINK / RMDIR | `unlink` / `rmdir` |
| RENAME / RENAME2 | `rename` |
| LINK | `link` |
| OPEN / RELEASE | `open` / `release` |
| READ / WRITE | `read` / `write` |
| FLUSH / FSYNC | `flush` / `fsync` |
| OPENDIR / RELEASEDIR | `opendir` / `releasedir` |
| READDIR / READDIRPLUS | `readdir` / `readdirplus` |
| FSYNCDIR | `fsyncdir` |
| STATFS | `statfs` |
| ACCESS | `access` |
| CREATE | `create` |
| FALLOCATE | `fallocate` |
| LSEEK | `lseek` |
| COPY_FILE_RANGE | `copy_file_range` |
| DESTROY | `destroy` |

## License

Licensed under [Apache License, Version 2.0](LICENSE).
