#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include "Task_2.h"

// cursor abstraction

// Create a cursor at the beginning of the table
// Create a cursor at the end of the table
// Access the row the cursor is pointing to
// Advance the cursor to the next row

// Use (in further tasks)

// Delete the row pointed to by a cursor
// Modify the row pointed to by a cursor
// Search a table for a given ID, and create a cursor pointing to the row with that ID

typedef struct
{
    Table *table;
    uint32_t row_num;
    bool end_of_table; // Indicates a position one past the last element
} Cursor;

Cursor *table_start(Table *table)
{
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = false;
}
Cursor *table_end(Table *table)
{
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;
}
// creates new cursors

// Now, row_slot() fxn renamed to cursor_location()
// modify implementation accordingly
void *cursor_location(Cursor *cursor)
{
    uint32_t page_num;
    uint32_t row_num = cursor->row_num;
    Table *table = cursor->table;
    if (row_num % ROWS_PER_PAGE != 0)
        page_num = row_num / ROWS_PER_PAGE;
    else
        page_num = row_num / ROWS_PER_PAGE - 1;
    void *page = table->pager->pages[page_num];
    if (page == NULL)
    {
        page = table->pager->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

// next row
void cursor_advance(Cursor *cursor)
{
    if (!(cursor->end_of_table))
    {
        cursor->row_num++;
        return cursor;
    }
    else
        printf("End of Table\n");
}

// use the above cursor fxns (instead of rows) in
// insert (table_end, etc.)
// select (table_begin, aursor_advance, etc.)