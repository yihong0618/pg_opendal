# Note it still vibe in progress

## Requirements

- PostgreSQL 13-17
- Rust 1.70+
- pgrx 0.14.3

### Install

```bash
cargo install cargo-pgrx
cargo pgrx init
cargo pgrx install
```

### Install in pg

```sql
CREATE EXTENSION pg_opendal;
```

### Examples

#### pg_opendal_read(service, path, config)

Read file content.

**Parameters:**

- `service` (text): Storage service type (e.g., 'fs', 's3', 'memory')
- `path` (text): File path
- `config` (jsonb): Service configuration

**Returns:** text - File content

**Examples:**

```sql

-- Read local file
SELECT pg_opendal_read('fs', '/tmp/test.txt', '{"root": "/"}');

-- Read S3 object
SELECT pg_opendal_read('s3', 'path/to/file.txt', '{
    "bucket": "my-bucket",
    "region": "us-east-1",
    "access_key_id": "your-access-key",
    "secret_access_key": "your-secret-key"
}');
```

#### pg_opendal_write(service, path, content, config)

Write file content.

**Parameters:**

- `service` (text): Storage service type
- `path` (text): File path
- `content` (text): Content to write
- `config` (jsonb): Service configuration

**Returns:** boolean - Returns true on success

**Examples:**

```sql
-- Write to local file
SELECT pg_opendal_write('fs', '/tmp/test.txt', 'Hello World!', '{"root": "/"}');

-- Write to S3 object
SELECT pg_opendal_write('s3', 'path/to/file.txt', 'Hello S3!', '{
    "bucket": "my-bucket",
    "region": "us-east-1",
    "access_key_id": "your-access-key",
    "secret_access_key": "your-secret-key"
}');
```

#### pg_opendal_exists(service, path, config)

Check if file exists.

**Parameters:**

- `service` (text): Storage service type
- `path` (text): File path
- `config` (jsonb): Service configuration

**Returns:** boolean - Returns true if file exists

**Examples:**

```sql
SELECT pg_opendal_exists('fs', '/tmp/test.txt', '{"root": "/"}');
```

#### pg_opendal_delete(service, path, config)

Delete file.

**Parameters:**

- `service` (text): Storage service type
- `path` (text): File path
- `config` (jsonb): Service configuration

**Returns:** boolean - Returns true on success

**Examples:**

```sql
SELECT pg_opendal_delete('fs', '/tmp/test.txt', '{"root": "/"}');
```

### Metadata Operations

#### pg_opendal_stat(service, path, config)

Get file metadata information.

**Parameters:**

- `service` (text): Storage service type
- `path` (text): File path
- `config` (jsonb): Service configuration

**Returns:** jsonb - JSON object containing file metadata

The returned JSON contains the following fields:

- `content_length`: File size in bytes
- `is_file`: Whether it's a file
- `is_dir`: Whether it's a directory
- `last_modified`: Last modified time (RFC3339 format)

**Examples:**

```sql
SELECT pg_opendal_stat('fs', '/tmp/test.txt', '{"root": "/"}');
```

### Directory Operations

#### pg_opendal_create_dir(service, path, config)

Create directory.

**Parameters:**

- `service` (text): Storage service type
- `path` (text): Directory path
- `config` (jsonb): Service configuration

**Returns:** boolean - Returns true on success

**Examples:**

```sql
SELECT pg_opendal_create_dir('fs', '/tmp/new_directory/', '{"root": "/"}');
```

#### pg_opendal_list(service, path, config)

List directory contents.

**Parameters:**

- `service` (text): Storage service type
- `path` (text): Directory path
- `config` (jsonb): Service configuration

**Returns:** jsonb[] - Array of directory entries

Each directory entry contains the following fields:

- `name`: File/directory name
- `path`: Full path
- `is_file`: Whether it's a file
- `is_dir`: Whether it's a directory
- `content_length`: File size
- `last_modified`: Last modified time

**Examples:**

```sql
SELECT pg_opendal_list('fs', '/tmp/', '{"root": "/"}');
```

#### pg_opendal_copy(service, source, target, config)

Copy file.

**Parameters:**

- `service` (text): Storage service type
- `source` (text): Source file path
- `target` (text): Target file path
- `config` (jsonb): Service configuration

**Returns:** boolean - Returns true on success

**Examples:**

```sql
SELECT pg_opendal_copy('fs', '/tmp/source.txt', '/tmp/target.txt', '{"root": "/"}');
```

#### pg_opendal_rename(service, source, target, config)

Rename/move file.

**Parameters:**

- `service` (text): Storage service type
- `source` (text): Source file path
- `target` (text): Target file path
- `config` (jsonb): Service configuration

**Returns:** boolean - Returns true on success

**Examples:**

```sql
SELECT pg_opendal_rename('fs', '/tmp/old_name.txt', '/tmp/new_name.txt', '{"root": "/"}');
```

### Service Capabilities

#### pg_opendal_capability(service, config)

Query storage service supported operation capabilities.

**Parameters:**

- `service` (text): Storage service type
- `config` (jsonb): Service configuration

**Returns:** jsonb - Service capability information

The returned JSON contains the following fields:

- `read`: Whether read is supported
- `write`: Whether write is supported
- `list`: Whether list is supported
- `stat`: Whether metadata query is supported
- `delete`: Whether delete is supported
- `copy`: Whether copy is supported
- `rename`: Whether rename is supported
- `create_dir`: Whether directory creation is supported

**Examples:**

```sql
SELECT pg_opendal_capability('s3', '{"bucket": "my-bucket", "region": "us-east-1"}');
```

## Configuration Examples

### Local File System

```sql
-- Configure access to local file system
SELECT pg_opendal_read('fs', 'test.txt', '{"root": "/home/user/data"}');
```

### Amazon S3

```sql
-- Using Access Key authentication
SELECT pg_opendal_read('s3', 'path/to/file.txt', '{
    "bucket": "my-bucket",
    "region": "us-east-1",
    "access_key_id": "xxxxxxxxxxxxxxxx",
    "secret_access_key": "xxxxxxxxxxxxxx"
}');
