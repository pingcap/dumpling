package export

type Task interface{}

type TaskDatabaseMeta struct {
	Task
	DatabaseName      string
	CreateDatabaseSQL string
}

type TaskTableMeta struct {
	Task
	DatabaseName   string
	TableName      string
	CreateTableSQL string
}

type TaskViewMeta struct {
	Task
	DatabaseName   string
	ViewName       string
	CreateTableSQL string
	CreateViewSQL  string
}

type TaskTableData struct {
	Task
	Meta        TableMeta
	Data        TableDataIR
	ChunkIndex  int
	TotalChunks int
}

func NewTaskDatabaseMeta(dbName, createSQL string) *TaskDatabaseMeta {
	return &TaskDatabaseMeta{
		DatabaseName:      dbName,
		CreateDatabaseSQL: createSQL,
	}
}

func NewTaskTableMeta(dbName, tblName, createSQL string) *TaskTableMeta {
	return &TaskTableMeta{
		DatabaseName:   dbName,
		TableName:      tblName,
		CreateTableSQL: createSQL,
	}
}

func NewTaskViewMeta(dbName, tblName, createTableSQL, createViewSQL string) *TaskViewMeta {
	return &TaskViewMeta{
		DatabaseName:   dbName,
		ViewName:       tblName,
		CreateTableSQL: createTableSQL,
		CreateViewSQL:  createViewSQL,
	}
}

func NewTaskTableData(meta TableMeta, data TableDataIR, totalChunks int) *TaskTableData {
	return &TaskTableData{
		Meta:        meta,
		Data:        data,
		ChunkIndex:  0,
		TotalChunks: totalChunks,
	}
}

func (t *TaskTableData) setCurrentChunkIndex(idx int) *TaskTableData {
	t.ChunkIndex = idx
	return t
}
