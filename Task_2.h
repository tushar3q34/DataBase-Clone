#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>


typedef enum { BUFFER_CREATED, BUFFER_NOT_CREATED } ReadInputStatus;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct
{
    StatementType type;
    Row row_to_insert; // only used by insert statement
} Statement;

typedef enum
{
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

typedef enum
{
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum
{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct
{
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

extern const uint32_t ID_SIZE;
extern const uint32_t USERNAME_SIZE;
extern const uint32_t EMAIL_SIZE;
extern const uint32_t ID_OFFSET;
extern const uint32_t USERNAME_OFFSET;
extern const uint32_t EMAIL_OFFSET;
extern const uint32_t ROW_SIZE;
extern const uint32_t PAGE_SIZE;

#define TABLE_MAX_PAGES 100

extern const uint32_t ROWS_PER_PAGE;
extern const uint32_t TABLE_MAX_ROWS;

void serialize_row(Row *source, void *destination);
void deserialize_row(void *source, Row *destination);

typedef struct
{
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct
{
    uint32_t num_rows;
    Pager *pager;
} Table;

void *get_page(Pager *pager, uint32_t page_num);

typedef struct
{
    Table *table;
    uint32_t row_num;
    bool end_of_table; // Indicates a position one past the last element
} Cursor;

Cursor *table_start(Table *table);
Cursor *table_end(Table *table);
void *cursor_value(Cursor *cursor);
void cursor_advance(Cursor *cursor);
void pager_flush(Pager *pager, uint32_t page_num, uint32_t size);
void db_close(Table *table);
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table);
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{
    int count = 0;
    char *string = input_buffer->buffer;
    char *token = strtok(string, " "); // Each token at a time
    char insert[7] = "insert";
    char select[7] = "select";
    if (strcmp(token, insert) == 0)
    {
        statement->type = STATEMENT_INSERT;
        while (token != NULL)
        {
            count++;

            token = strtok(NULL, " ");
            if (token != NULL)
            {
                if (count == 1) // First argument of insert
                {
                    char *ptr;
                    int num = strtol(token, &ptr, 10);
                    if (num >= 0)
                    {
                        statement->row_to_insert.id = (uint32_t)num;
                        return PREPARE_SUCCESS;
                    }
                    else
                        return PREPARE_NEGATIVE_ID;
                }
                if (count == 2) // Second argument of insert
                {
                    if (strlen(token) > COLUMN_USERNAME_SIZE)
                        return PREPARE_STRING_TOO_LONG;
                    else
                    {
                        strcpy(statement->row_to_insert.username, token);
                        return PREPARE_SUCCESS;
                    }
                }
                if (count == 3) // Third argument of insert
                {
                    if (strlen(token) > COLUMN_EMAIL_SIZE)
                        return PREPARE_STRING_TOO_LONG;
                    else
                    {
                        strcpy(statement->row_to_insert.email, token);
                        return PREPARE_SUCCESS;
                    }
                }
                if (count == 4) // If more than 3 arguments then incorrect num of arguments
                    return PREPARE_SYNTAX_ERROR;
            }
        }
        if (count < 4) // Similarly if less arguments
            return PREPARE_SYNTAX_ERROR;
    }
    else if (strcmp(token, select) == 0)
        statement->type = STATEMENT_SELECT; // For select statement
    else
        return PREPARE_UNRECOGNIZED_STATEMENT;
}
Pager *pager_open(const char *filename);
Table *db_open(const char *filename);
void print_row(Row *row);
ExecuteResult execute_insert(Statement *statement, Table *table);
ExecuteResult execute_select(Statement *statement, Table *table);
ExecuteResult execute_statement(Statement *statement, Table *table);
InputBuffer *new_input_buffer();
ReadInputStatus read_input(InputBuffer *input_buffer);
void close_input_buffer(InputBuffer *input_buffer);
