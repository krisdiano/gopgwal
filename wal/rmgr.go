package wal

import "fmt"

type RmgrId uint8

var name = map[RmgrId]string{
	RM_XLOG_ID:       "XLOG",
	RM_XACT_ID:       "Transaction",
	RM_SMGR_ID:       "Storage",
	RM_CLOG_ID:       "CLOG",
	RM_DBASE_ID:      "Database",
	RM_TBLSPC_ID:     "Tablespace",
	RM_MULTIXACT_ID:  "MultiXact",
	RM_RELMAP_ID:     "RelMap",
	RM_STANDBY_ID:    "Standby",
	RM_HEAP2_ID:      "Heap2",
	RM_HEAP_ID:       "Heap",
	RM_BTREE_ID:      "Btree",
	RM_HASH_ID:       "Hash",
	RM_GIN_ID:        "Gin",
	RM_GIST_ID:       "Gist",
	RM_SEQ_ID:        "Sequence",
	RM_SPGIST_ID:     "SPGist",
	RM_BRIN_ID:       "BRIN",
	RM_COMMIT_TS_ID:  "CommitTs",
	RM_REPLORIGIN_ID: "ReplicationOrigin",
	RM_GENERIC_ID:    "Generic",
	RM_LOGICALMSG_ID: "LogicalMessage",
}

func RmgrIdName(id RmgrId) string {
	v, ok := name[id]
	if !ok {
		return fmt.Sprintf("unknown %d", id)
	}
	return v
}

const (
	RM_XLOG_ID RmgrId = iota
	RM_XACT_ID
	RM_SMGR_ID
	RM_CLOG_ID
	RM_DBASE_ID
	RM_TBLSPC_ID
	RM_MULTIXACT_ID
	RM_RELMAP_ID
	RM_STANDBY_ID
	RM_HEAP2_ID
	RM_HEAP_ID
	RM_BTREE_ID
	RM_HASH_ID
	RM_GIN_ID
	RM_GIST_ID
	RM_SEQ_ID
	RM_SPGIST_ID
	RM_BRIN_ID
	RM_COMMIT_TS_ID
	RM_REPLORIGIN_ID
	RM_GENERIC_ID
	RM_LOGICALMSG_ID
)
