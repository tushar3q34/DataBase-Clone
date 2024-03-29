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

void print_prompt() { printf("db > "); }

void serialize_row(Row *source, void *destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->id), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->id), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->id), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->id), source + EMAIL_OFFSET, EMAIL_SIZE);
}

// Struct defns given in Task2.h
// Function declarations are also given in Task2.h
// You need not use the same fxns, modify according to your implementation
// and edit the declarations in .h later
// You can add/delete things from .h file according to your code

// add all fxns from task-1 here
void pager_flush(Pager *pager, uint32_t page_num, uint32_t size)
{
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    int status = write(pager->file_descriptor, pager->pages[page_num], (size_t)size);
    if (status == -1)
        printf("Error while flushing pages\n");
    if (status == 0)
        printf("Reached the end of file\n");
    pager->pages[page_num] = NULL;
}

void db_close(Table *table)
{
    int i;
    for (i = 0; i < TABLE_MAX_PAGES; i++)
    {
        if ((table->pager)->pages[i] != NULL)
        {
            if ((table->num_rows) - (i + 1) * ROWS_PER_PAGE >= 0)
                pager_flush(table->pager, i, PAGE_SIZE);
            else
                pager_flush(table->pager, i, ((table->num_rows) % ROWS_PER_PAGE) * ROW_SIZE);
        }
    }
    close((table->pager)->file_descriptor);
    free(table->pager);
    free(table);
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table)
{
    if (!strcmp((input_buffer->buffer + 1), "exit "))
    {
        close_input_buffer(input_buffer);
        db_close(table);
        return META_COMMAND_SUCCESS;
    }
    else
        return META_COMMAND_UNRECOGNIZED_COMMAND;
}

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

Pager *pager_open(const char *filename)
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

Table *db_open(const char *filename)
{
    Table *table = (Table *)calloc(1, sizeof(Table));
    table->pager = pager_open(filename);
    table->num_rows = ((table->pager)->file_length) / ROW_SIZE;
    return table;
}

ExecuteResult execute_statement(Statement *statement, Table *table)
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

ReadInputStatus read_input(InputBuffer *input_buffer)
{
    /*
        @mandeep check and update the function as needed
    */

    input_buffer->input_length = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

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

void close_input_buffer(InputBuffer *input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}





void *get_page(Pager *pager, uint32_t page_num)
{
    void *page = pager->pages[page_num];
    if (page == NULL)
    { // new page
        // Cache miss. Allocate memory and load from file.
        // Allocate memory only when we try to access page
        page = pager->pages[page_num] = malloc(ROWS_PER_PAGE * ROW_SIZE);
    }
    return page;
}

// add pager fxns with the functionalities given in Pager_template
// add cursor fxns with the functionalities given in Cursor_template

int main(int argc, char *argv[])
{
}
