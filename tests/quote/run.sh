#!/bin/sh

set -eu

function prepare_sql_file() {
    echo "prepare quo\`te-database-schema-create.sql"
    echo "CREATE DATABASE \`quo\`\`te-database\` /*!40100 DEFAULT CHARACTER SET latin1 */;"> "$DUMPLING_BASE_NAME/data/quo\`te-database-schema-create.sql"
    echo "prepare quo\`te-database.quo\`te-table-schema.sql"
    echo "CREATE TABLE \`quo\`\`te-table\` (
  \`quo\`\`te-col\` int(11) NOT NULL,
  \`a\` int(11) DEFAULT NULL,
  \`gen\`\`id\` int(11) GENERATED ALWAYS AS (\`quo\`\`te-col\`) VIRTUAL,
  PRIMARY KEY (\`quo\`\`te-col\`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"> "$DUMPLING_BASE_NAME/data/quo\`te-database.quo\`te-table-schema.sql"
    echo "prepare quo\`te-database.quo\`te-table.0.sql"
    echo "/*!40101 SET NAMES binary*/;
INSERT INTO \`quo\`\`te-table\` (\`quo\`\`te-col\`,\`a\`) VALUES
(0,10),
(1,9),
(2,8),
(3,7),
(4,6),
(5,5),
(6,4),
(7,3),
(8,2),
(9,1),
(10,0);"> "$DUMPLING_BASE_NAME/data/quo\`te-database.quo\`te-table.0.sql"
}

prepare_sql_file

db="quo\`te-database"
run_sql "drop database if exists \`quo\`\`te-database\`"
run_sql_file "$DUMPLING_BASE_NAME/data/quo\`te-database-schema-create.sql"
export DUMPLING_TEST_DATABASE=$db

run_sql_file "$DUMPLING_BASE_NAME/data/quo\`te-database.quo\`te-table-schema.sql"
run_sql_file "$DUMPLING_BASE_NAME/data/quo\`te-database.quo\`te-table.0.sql"

run_dumpling

for file_path in "$DUMPLING_BASE_NAME"/data/*; do
  base_name=$(basename "$file_path")
  file_should_exist "$DUMPLING_BASE_NAME/data/$base_name"
  file_should_exist "$DUMPLING_OUTPUT_DIR/$base_name"
  diff "$DUMPLING_BASE_NAME/data/$base_name" "$DUMPLING_OUTPUT_DIR/$base_name"
done
