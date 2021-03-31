package seatable_api

type ColumnTypes string

var ROW_FILTER_KEYS []string = []string{"column_name", "filter_predicate", "filter_term", "filter_term_modifier"}

const (
	RENAME_COLUMN      = "rename_column"
	RESIZE_COLUMN      = "resize_column"
	FREEZE_COLUMN      = "freeze_column"
	MOVE_COLUMN        = "move_column"
	MODIFY_COLUMN_TYPE = "modify_column_type"
	DELETE_COLUMN      = "delete_column"

	JOIN_ROOM        = "join-room"
	UPDATE_DTABLE    = "update-dtable"
	NEW_NOTIFICATION = "new-notification"
)
