package seatable_api

type ColumnTypes string

var ROW_FILTER_KEYS []string = []string{"column_name", "filter_predicate", "filter_term", "filter_term_modifier"}

const (
	NUMBER          ColumnTypes = "number"
	TEXT            ColumnTypes = "text"
	CHECKBOX        ColumnTypes = "checkbox"
	DATE            ColumnTypes = "date"
	SINGLE_SELECT   ColumnTypes = "single-select"
	LONG_TEXT       ColumnTypes = "long-text"
	IMAGE           ColumnTypes = "image"
	FILE            ColumnTypes = "file"
	MULTIPLE_SELECT ColumnTypes = "multiple-select"
	COLLABORATOR    ColumnTypes = "collaborator"
	LINK            ColumnTypes = "link"
	FORMULA         ColumnTypes = "formula"
	CREATOR         ColumnTypes = "creator"
	CTIME           ColumnTypes = "ctime"
	LAST_MODIFIER   ColumnTypes = "last-modifier"
	MTIME           ColumnTypes = "mtime"
	GEOLOCATION     ColumnTypes = "geolocation"
	AUTO_NUMBER     ColumnTypes = "auto-number"
	URL             ColumnTypes = "url"

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
