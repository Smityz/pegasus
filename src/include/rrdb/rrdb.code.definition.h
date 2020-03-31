#pragma once
#include <dsn/dist/replication/replication.codes.h>

namespace dsn {
namespace apps {
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_PUT, ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_MULTI_PUT, NOT_ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_REMOVE, ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_MULTI_REMOVE, NOT_ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_INCR, NOT_ALLOW_BATCH, NOT_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_CHECK_AND_SET, NOT_ALLOW_BATCH, NOT_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_CHECK_AND_MUTATE, NOT_ALLOW_BATCH, NOT_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_RRDB_RRDB_DUPLICATE, NOT_ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_GET)
DEFINE_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_MULTI_GET)
DEFINE_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_SORTKEY_COUNT)
DEFINE_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_TTL)
DEFINE_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_GET_SCANNER)
DEFINE_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_SCAN)
DEFINE_STORAGE_READ_RPC_CODE(RPC_RRDB_RRDB_CLEAR_SCANNER)
} // namespace apps
} // namespace dsn
