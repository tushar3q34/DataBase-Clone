#include "Task_2.h"
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

const uint32_t ID_SIZE = sizeof(uint32_t);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_SIZE = 32;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_SIZE = 255;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;

void print_prompt() { printf("db > "); }

void serialize_row(Row *source, void *destination) // Serializing data to pages
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination) // Deserializing data from pages
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *row_slot(Table *table, uint32_t row_num) // Slotting of row to page
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    if (!(row_num % ROWS_PER_PAGE))
        page_num--;
    void *page = get_page(table, page_num);
    uint32_t row_offset = (row_num - 1) % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size) // Writing to db and freeing heap mem
{
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    int status =
        pwrite(pager->file_descriptor, pager->pages[page_num], (size_t)size, offset);
    if (status == -1)
        printf("Error while flushing pages\n");
    if (status == 0)
        printf("Reached the end of file\n");
    free(pager->pages[page_num]);
    pager->pages[page_num] = NULL;
}

void db_close(Table *table) // Close db
{
    for (int i = 0; i < TABLE_MAX_PAGES; i++)
    {
        if ((table->pager)->pages[i] != NULL) // Writing to db only from those pages which were accessed or written
        {
            if (((int)table->num_rows - (int)((i + 1) * ROWS_PER_PAGE)) >= 0)
            {
                pager_flush(table->pager, i, PAGE_SIZE);
            }
            else
            {
                pager_flush(table->pager, i,
                            ((table->num_rows) % ROWS_PER_PAGE) * ROW_SIZE);
            }
        }
    }
    close((table->pager)->file_descriptor);
    free(table->pager);
    free(table);
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) // Meta command - .exit
{
    if (!strcmp((input_buffer->buffer), ".exit"))
    {
        close_input_buffer(&input_buffer);
        db_close(table);
        return META_COMMAND_SUCCESS;
    }
    else
        return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_statement(InputBuffer *input_buffer,
                                Statement *statement) // Categorizing statement into insert or select
{
    int count = 0;
    char *string = input_buffer->buffer;
    char *token = strtok(string, " ");
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
                if (count == 1)
                {
                    char *ptr;
                    int num = strtol(token, &ptr, 10);
                    if (num >= 0)
                    {
                        statement->row_to_insert.id = (uint32_t)num;
                    }
                    else
                    {
                        return PREPARE_NEGATIVE_ID;
                    }
                }
                else if (count == 2)
                {
                    if (strlen(token) > COLUMN_USERNAME_SIZE)
                        return PREPARE_STRING_TOO_LONG;
                    else
                        strcpy(statement->row_to_insert.username, token);
                }
                else if (count == 3)
                {
                    if (strlen(token) > COLUMN_EMAIL_SIZE)
                        return PREPARE_STRING_TOO_LONG;
                    else
                        strcpy(statement->row_to_insert.email, token);
                }
                else
                {
                    return PREPARE_SYNTAX_ERROR;
                }
            }
        }
        if (count < 3)
        {
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    else if (strcmp(token, select) == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    else
    {
        return PREPARE_UNRECOGNIZED_STATEMENT;
    }
}

Pager *pager_open(const char *filename) // Opening file and allocating mem for pager
{
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }
    Pager *temp = (Pager *)calloc(1, sizeof(Pager));
    temp->file_descriptor = fd;
    struct stat st;
    if (stat(filename, &st) == 0)
        temp->file_length = st.st_size;
    return temp;
}

Table *db_open(const char *filename) // Allocating memory for Table struct
{
    Table *table = (Table *)calloc(1, sizeof(Table));
    table->pager = pager_open(filename);
    table->num_rows = ((table->pager)->file_length) / ROW_SIZE;
    Pager *pager = table->pager;
    return table;
}

ExecuteResult execute_statement(Statement *statement, Table *table) // Executing statements
{

    if (statement->type == STATEMENT_INSERT) // For insert
    {

        Row *row_to_insert = &(statement->row_to_insert);
        if (table->num_rows < (ROWS_PER_PAGE * TABLE_MAX_PAGES))
        {

            table->num_rows++;
            serialize_row(row_to_insert, row_slot(table, table->num_rows));
            return EXECUTE_SUCCESS;
        }
        else
        {
            printf("TABLE FULL !!\n");
            return EXECUTE_TABLE_FULL;
        }
    }
    else if (statement->type == STATEMENT_SELECT) // For select
    {
        Row row;
        for (int i = 1; i <= table->num_rows; i++)
        {
            deserialize_row(row_slot(table, i), &row);
            printf("(%d , %s , %s)\n", row.id, row.username, row.email);
        }
        return EXECUTE_SUCCESS;
    }
    return EXECUTE_FAIL;
}

InputBuffer *new_input_buffer() // Initializing Input buffer
{
    InputBuffer *input_buffer = calloc(1, sizeof(InputBuffer));
    return input_buffer;
}

ReadInputStatus read_input(InputBuffer *input_buffer) // Taking input from user
{

    input_buffer->input_length =
        getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (input_buffer->input_length == -1)
    {
        puts("ERROR WHILE GETTING INPUT (GETLINE)");
        return BUFFER_NOT_CREATED;
    }
    else
    {
        input_buffer->buffer[input_buffer->input_length - 1] = '\0';
        return BUFFER_CREATED;
    }
}

void close_input_buffer(InputBuffer **input_buffer) // Deallocating input buffer
{
    free((*input_buffer)->buffer);
    free((*input_buffer));
    *input_buffer = NULL;
}

void *get_page(Table *table, uint32_t page_num) // Reading data from db to pages
{
    void *page = (table->pager)->pages[page_num];
    if (page == NULL)
    {
        if (((int)table->num_rows - (int)((page_num + 1) * ROWS_PER_PAGE)) >= 0)
        {
            // full page
            page = (table->pager)->pages[page_num] = malloc(ROWS_PER_PAGE * ROW_SIZE);
            read((table->pager)->file_descriptor, page, (size_t)ROWS_PER_PAGE * ROW_SIZE);
        }
        else
        {
            // partial page
            page = (table->pager)->pages[page_num] = malloc(((table->num_rows) % ROWS_PER_PAGE) * ROW_SIZE);
            read((table->pager)->file_descriptor, page, (size_t)((table->num_rows) % ROWS_PER_PAGE) * ROW_SIZE);
        }
    }
    return page;
}

// We have not included Cursor.c to Task_2.c as the only implementation of Cursor.c this time is
// using it instead of row_slot. Functions are ready in Cursor.c -> Will implement them next time

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        printf("Give a database filename.\n");
        exit(EXIT_FAILURE);
    }
    char *filename = argv[1];
    Table *table = db_open(filename);

    InputBuffer *input_buffer = new_input_buffer();
    if (input_buffer == NULL || table == NULL)
    {
        printf("Memory allocation failed.\n");
        exit(EXIT_FAILURE);
    }
    while (true)
    {
        print_prompt();
        ReadInputStatus status = read_input(input_buffer);
        if (status == BUFFER_NOT_CREATED)
        {
            printf("Error reading input\n");
            continue;
        }
        if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_command(input_buffer, table))
            {
            case META_COMMAND_SUCCESS:
                exit(EXECUTE_SUCCESS);
            case META_COMMAND_UNRECOGNIZED_COMMAND:
                printf("Unrecognized command '%s'\n", input_buffer->buffer);
                continue;
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement))
        {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_NEGATIVE_ID:
            printf("ID must be positive\n");
            continue;
        case PREPARE_STRING_TOO_LONG:
            printf("String is too long\n");
            continue;
        case PREPARE_SYNTAX_ERROR:
            printf("Syntax error\n");
            continue;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            printf("Unrecognized keyword '%s'.\n", input_buffer->buffer);
            continue;
        }
        switch (execute_statement(&statement, table))
        {
        case EXECUTE_SUCCESS:
            printf("Executed.\n");
            break;
        case EXECUTE_TABLE_FULL:
            printf("Table full.\n");
            break;
        case EXECUTE_FAIL:
            printf("Execution failed.\n");
            break;
        }
    }
    return 0;
}
