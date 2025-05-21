package pgindexhealth

import (
	"fmt"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

type SqlChecker struct {
	conn      *sqlx.DB
	schemaName string
	bloatLimit float64
	RemainingPercentageThreshold float64
}

type SqlConf struct {
	Username   string
	Password   string
	Addr       string
	Port       string
	Dbname     string
	SchemaName string
}

type CheckerConf struct {
	SqlConf
	BloatLimit float64
	RemainingPercentageThreshold float64
}

func NewSqlChecker(conf *CheckerConf) (*SqlChecker, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", conf.Username, conf.Password, conf.Addr, conf.Port, conf.Dbname)
	db, err := sqlx.Connect("pgx", connStr)

	if err != nil {
		return &SqlChecker{nil, "", conf.BloatLimit, conf.RemainingPercentageThreshold}, fmt.Errorf("UNABLE TO CONNECT TO DB: %v", err)
	}
	if conf.SchemaName != "" {
		return &SqlChecker{db, conf.SchemaName, conf.BloatLimit, conf.RemainingPercentageThreshold}, nil
	}
	return &SqlChecker{db, "public", conf.BloatLimit, conf.RemainingPercentageThreshold}, nil
}

func (sc *SqlChecker) CloseConn() error {
	if sc.conn == nil {
		return fmt.Errorf("NOTHING TO CLOSE")
	}

	err := sc.conn.Close()

	if err != nil {
		return err
	}
	return nil
}

func queryFromFile(conn *sqlx.DB, filename string, args ...any) (*sqlx.Rows, error) {
	b, err := os.ReadFile("sql/" + filename)
	if err != nil {
		return nil, fmt.Errorf("ERROR READING FILE %s", err.Error())
	}
	str := string(b[:])

	rows, err := conn.Queryx(str, args...)
	
	if err != nil {
		return nil, fmt.Errorf("ERROR DB %s", err.Error())
	}
	
	return rows, nil 
}

func check[T any](sc *SqlChecker, queryFile string, errorText string, args ...any) ([]T, error) {
	var tRows *sqlx.Rows
	var err error
	if len (args) == 0 {
		tRows, err = queryFromFile(sc.conn, queryFile, sc.schemaName)
	} else {
		args := append([]any{sc.schemaName}, args...)
		tRows, err = queryFromFile(sc.conn, queryFile, args...)
	}
	checkName := strings.Split(queryFile, ".sql")[0]

	if err != nil {
		return []T{}, fmt.Errorf("ERROR AT %s : %s", checkName, err.Error())
	}

	defer tRows.Close()

	tArr := make([]T, 0)

	for tRows.Next() {
		var tmp T
		err := tRows.StructScan(&tmp) 
		
		if err != nil {
			return []T{}, fmt.Errorf("ERROR AT %s: %s", checkName, err.Error())
		}
		
		tArr = append(tArr, tmp)
	}

	if tRows.Err() != nil {
		return []T{}, fmt.Errorf("ERROR AT %s: %s", checkName, tRows.Err().Error())
	}

	if len(tArr) != 0 {
		errorString := errorText
		for idx, tVal := range tArr {
			errorString += fmt.Sprintf("#%d: %#v\n", idx, tVal)
		}
		return tArr, fmt.Errorf("%s", errorString)
	}
	return []T{}, nil
}

type Table struct {
	TableName string `db:"table_name"`
	TableSize int `db:"table_size"`
}

type Index struct {
	TableName string `db:"table_name"`
	IndexName string `db:"index_name"`
	IndexSize uint `db:"index_size"`
}

type bloatedTable struct {
	Table
	BloatSize         int `db:"bloat_size"`
	BloatPercent      float64 `db:"bloat_percentage"`
	StatsNotAvailable bool `db:"stats_not_available"`
}

type bloatedIndex struct {
	Index
	BloatSize         int `db:"bloat_size"`
	BloatPercent      float64 `db:"bloat_percentage"`
	StatsNotAvailable bool `db:"stats_not_available"`
}

type TableWithoutDescription struct {
	Name string `db:"table_name"`
}

func (sc *SqlChecker) CheckTablesWithoutDescription() ([]TableWithoutDescription, error) {
	return check[TableWithoutDescription](sc, "tables_without_description.sql", "TABLES WITHOUT DESCRIPTION: ")
}

func (sc *SqlChecker) CheckBloatedIndexes() ([]bloatedIndex, error) {
	return check[bloatedIndex](sc, "bloated_indexes.sql", "DETECTED BLOATED INDEXES: ", sc.bloatLimit)
}

func (sc *SqlChecker) CheckBloatedTables() ([]bloatedTable, error) {
	return check[bloatedTable](sc, "bloated_tables.sql", "DETECTED BLOATED TABLES: ", sc.bloatLimit)
}

type BTreeIndexOnArrayColumn struct {
	TableName     string `db:"table_name"`
	IndexName     string `db:"index_name"`
	ColumnNotNull bool `db:"column_not_null"`
	ColumnName    string `db:"column_name"`
	IndexSize     int `db:"index_size"`
}

func (sc *SqlChecker) CheckBTreeIndexesOnArrayColumns() ([]BTreeIndexOnArrayColumn, error) {
	return check[BTreeIndexOnArrayColumn](sc, "btree_indexes_on_array_columns.sql", "DETECTED B-Tree INDEXES ON ARRAY COLUMNS: ")
}

type Column struct {
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
}

func (sc *SqlChecker) CheckColumnsNotFollowingNamingConvention() ([]Column, error) {
	return check[Column](sc, "columns_not_following_naming_convention.sql", "DETECTED COLUMNS NOT FOLLOWING NAME CONVENTION: ")
}

func (sc *SqlChecker) CheckColumnsWithFixedLengthVarchar() ([]Column, error) {
	return check[Column](sc, "columns_with_fixed_length_varchar.sql", "DETECTED COLUMNS WITH FIXED LENGTH VARCHAR: ")
}

func (sc *SqlChecker) CheckColumnsWithJsonType() ([]Column, error) {
	return check[Column](sc, "columns_with_json_type.sql", "DETECTED COLUMNS WITH JSON TYPE: ")
}

func (sc *SqlChecker) CheckColumnsWithoutDescription() ([]Column, error) {
	return check[Column](sc, "columns_without_description.sql", "DETECTED COLUMNS WITHOUT DESCRIPTION: ")
}

func (sc *SqlChecker) CheckTablesNotLinkedToOthers() ([]Table, error) {
	return check[Table](sc, "tables_not_linked_to_others.sql", "DETECTED TABLES NOT LINKED TO OTHERS: ")
}

func (sc *SqlChecker) CheckTablesWithZeroOrOneColumns() ([]Table, error) {
	return check[Table](sc, "tables_with_zero_or_one_column.sql", "DETECTED TABLES WITH ZERO OR ONE COLUMN: ")
}

func (sc *SqlChecker) CheckTablesWithoutPK() ([]Table, error) {
	return check[Table](sc, "tables_without_primary_key.sql", "DETECTED TABLES WITHOUT PRIMARY KEY: ")
}

type TableWithMissingIndexes struct {
	Table
	SeqScan int `db:"seq_scan"`
	IdxScan int `db:"idx_scan"`
}

func (sc *SqlChecker) CheckTablesWithMissingIndexes() ([]TableWithMissingIndexes, error) {
	return check[TableWithMissingIndexes](sc, "tables_with_missing_indexes.sql", "DETECTED TABLES WITH MISSING INDEXES: ")
}

func (sc *SqlChecker) CheckInvalidIndexes() ([]Index, error) {
	return check[Index](sc, "invalid_indexes.sql", "DETECTED INVALID INDEXES: ")
}

type UnusedIndex struct {
	Index
	IndexScans int `db:"index_scans"`
}

func (sc *SqlChecker) CheckUnusedIndexes() ([]UnusedIndex, error) {
	return check[UnusedIndex](sc, "unused_indexes.sql", "DETECTED UNUSED INDEXES: ")
}

type SerialColumn struct {
	Column
	ColumnType   string `db:"column_type"`
	SequenceName string `db:"sequence_name"`
}

func (sc *SqlChecker) CheckSerialColumns() ([]SerialColumn, error) {
	return check[SerialColumn](sc, "columns_with_serial_types.sql", "DETECTED SERIAL COLUMNS: ")
}

type BoolColumnIndex struct {
	Index
	ColumnNotNull bool `db:"column_not_null"`
	ColumnName    string `db:"column_name"`
}

func (sc *SqlChecker) CheckIndexesWithBoolean() ([]BoolColumnIndex, error) {
	return check[BoolColumnIndex](sc, "indexes_with_boolean.sql", "DETECTED INDEXES WITH BOOLEAN: ")
}

type IntersectedIndex struct {
	TableName          string `db:"table_name"`
	IntersectedIndexes string `db:"intersected_indexes"`
}

func (sc *SqlChecker) CheckIntersectedIndexes() ([]IntersectedIndex, error) {
	return check[IntersectedIndex](sc, "intersected_indexes.sql", "DETECTED INTERSECTED INDEXES: ")
}

type IndexWithNullValues struct {
	Index
	NullableFields string `db:"nullable_fields"`
}

func (sc *SqlChecker) CheckIndexesWithNullValues() ([]IndexWithNullValues, error) {
	return check[IndexWithNullValues](sc, "indexes_with_null_values.sql", "DETECTED INDEXES WITH NULL VALUES: ")
}

type IndexWithUnnecessaryWhereClause struct {
	Index
	Columns   []string `db:"columns"`
}

func (sc *SqlChecker) CheckIndexesWithUnnecessaryWhereClause() ([]IndexWithUnnecessaryWhereClause, error) {
	return check[IndexWithUnnecessaryWhereClause](sc, "indexes_with_unnecessary_where_clause.sql", "DETECTED INDEXES WITH UNNECESSARY WHERE CLAUSE: ")
}

type DuplicatedIndex struct {
	TableName         string `db:"table_name"`
	DuplicatedIndexes string `db:"duplicated_indexes"`
}

func (sc *SqlChecker) CheckDuplicatedIndexes() ([]DuplicatedIndex, error) {
	return check[DuplicatedIndex](sc, "duplicated_indexes.sql", "DETECTED DUPLICATED INDEXES: ")
}

type FunctionWithoutDescription struct {
	Name        string `db:"function_name"`
	Signature   string `db:"function_signature"`
}

func (sc *SqlChecker) CheckFunctionsWithoutDescription() ([]FunctionWithoutDescription, error) {
	return check[FunctionWithoutDescription](sc, "functions_without_description.sql", "DETECTED FUNCTIONS WITHOUT DESCRIPTION: ")
}

type Constraint struct {
	TableName                  string `db:"table_name"`
	ConstraintName             string `db:"constraint_name"`
	Columns                    string `db:"columns"`
}

type DuplicatedForeignKey struct {
	Constraint
	DuplicateContraintName     string `db:"duplicate_contraint_name"`
	DuplicateConstraintColumns []string `db:"duplicate_constraint_columns"`
}

func (sc *SqlChecker) CheckDuplicatedForeignKeys() ([]DuplicatedForeignKey, error) {
	return check[DuplicatedForeignKey](sc, "duplicated_foreign_keys.sql", "DETECTED DUPLICATED FOREIGN KEYS: ")
}

type intersectedForeignKey struct {
	Constraint
	IntersectedContraintName     string `db:"intersected_constraint_name"`
	IntersectedConstraintColumns []string `db:"intersected_constraint_columns"`
}

func (sc *SqlChecker) CheckIntersectedForeignKeys() ([]intersectedForeignKey, error) {
	return check[intersectedForeignKey](sc, "intersected_foreign_keys.sql", "DETECTED INTERSECTED FOREIGN KEYS: ")
}

func (sc *SqlChecker) CheckFKWithoutIndex() ([]Constraint, error) {
	return check[Constraint](sc, "foreign_keys_without_index.sql", "DETECTED FOREIGN KEYS WITHOUT INDEXES: ")
}

func (sc *SqlChecker) CheckFKWithUnmatchedColumnType() ([]Constraint, error) {
	return check[Constraint](sc, "foreign_keys_with_unmatched_column_type.sql", "DETECTED FOREIGN KEYS WITH UNMATCHED COLUMN TYPE: ")
}

type InvalidConstraint struct {
	TableName      string `db:"table_name"`
	ConstraintType string `db:"constraint_type"`
	Name           string `db:"constraint_name"`
}

func (sc *SqlChecker) CheckNotValidConstraints() ([]InvalidConstraint, error) {
	return check[InvalidConstraint](sc, "not_valid_constraints.sql", "DETECTED INVALID CONSTRAINTS: ")
}

type Object struct {
	ObjectName string `db:"object_name"`
	ObjectType string `db:"object_type"`
}

func (sc *SqlChecker) CheckPossibleObjectNameOverflow() ([]Object, error) {
	return check[Object](sc, "possible_object_name_overflow.sql", "DETECTED POSSIBLE OBJECT NAME OVERFLOW: ")
}

func (sc *SqlChecker) CheckObjectsNotFollowingNamingConvention() ([]Object, error) {
	return check[Object](sc, "objects_not_following_naming_convention.sql", "DETECTED OBJECTS NOT FOLLOWING NAME CONVENTIONS: \n")
}

type SequenceOverflow struct {
	SequenceName        string `db:"sequence_name"`
	DataType            string `db:"data_type"`
	RemainingPercentage float64 `db:"remaining_percentage"`
}

func (sc *SqlChecker) CheckSequenceOverflow() ([]SequenceOverflow, error) {
	return check[SequenceOverflow](sc, "sequence_overflow.sql", "DETECTED SEQUENCE OVERFLOW: ", sc.RemainingPercentageThreshold)
}

type PrimaryKeyWithSerialType struct {
	TableName     string `db:"table_name"`
	ColumnName    string `db:"column_name"`
	ColumnNotNull bool `db:"column_not_null"`
	ColumnType    string `db:"column_type"`
	SequenceName  string `db:"sequence_name"`
}

func (sc *SqlChecker) CheckPrimaryKeysWithSerialTypes() ([]PrimaryKeyWithSerialType, error) {
	return check[PrimaryKeyWithSerialType](sc, "primary_keys_with_serial_types.sql", "DETECTED PRIMARY KEY WITH SERIAL TYPE: ")
}

type PrimaryKeyWithUUIDVarchar struct {
	TableName     string `db:"table_name"`
	Columns       []string `db:"columns"`
}

func (sc *SqlChecker) CheckPrimaryKeyWithUUIDVarchar() ([]PrimaryKeyWithUUIDVarchar, error) {
	return check[PrimaryKeyWithUUIDVarchar](sc, "primary_keys_with_varchar.sql", "DETECTED PRIMARY KEY WITH UUID VARCHAR: ")
}
