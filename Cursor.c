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

// Used row_slot() for now, will use cursor_location() from next task onwards
void *cursor_location(Cursor *cursor) // Old row_slot() renamed to cursor_location
{
    uint32_t page_num = cursor->row_num / ROWS_PER_PAGE;
    if (!(cursor->row_num % ROWS_PER_PAGE))
        page_num--;
    void *page = get_page(cursor->table, page_num);
    uint32_t row_offset = (cursor->row_num - 1) % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

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

// Further implementation later ....